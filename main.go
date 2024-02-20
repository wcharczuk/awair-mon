package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/wcharczuk/go-incr"
)

// these may change based on DHCP settings.
var awairSensors = map[string]string{
	"Bedroom":     "192.168.4.74",
	"Living Room": "192.168.4.47",
	"Office":      "192.168.4.46",
}

func main() {
	g := incr.New()
	graphs := make(map[string]*SensorGraph)
	for sensor := range awairSensors {
		graphs[sensor] = createSensorGraph(g, sensor, 48*time.Hour)
	}

}

func createSensorGraph(g *incr.Graph, name string, windowLength time.Duration) *SensorGraph {
	output := SensorGraph{
		Name: name,
	}

	output.Latest = incr.Var[*Awair](g, nil)
	windowData := new(Queue[Awair])
	window := incr.Map(g, output.Latest, func(sample *Awair) []Awair {
		if sample == nil {
			return nil
		}
		windowData.Push(*sample)
		cutoff := time.Now().Add(-windowLength)
		for windowData.Len() > 0 {
			head, _ := windowData.Peek()
			if head.Timestamp.After(cutoff) {
				break
			}
			windowData.Pop()
		}
		return windowData.Values()
	})
	output.Window = incr.MustObserve(g, window)

	elapsedTimes := incr.Map(g, window, func(data []Awair) []time.Duration {
		output := make([]time.Duration, 0, len(data))
		for _, d := range data {
			output = append(output, d.Elapsed)
		}
		return output
	})
	sortedElapsedTimes := incr.Map(g, elapsedTimes, func(data []time.Duration) []time.Duration {
		output := make([]time.Duration, len(data))
		copy(output, data)
		slices.Sort(output)
		return output
	})
	minElapsedTime := cutoffEpsilonDuration(g, incr.Map(g, sortedElapsedTimes, func(data []time.Duration) time.Duration {
		if len(data) > 0 {
			return data[0]
		}
		return 0
	}), time.Millisecond)
	lastElapsedTime := cutoffEpsilonDuration(g, incr.Map(g, elapsedTimes, func(data []time.Duration) time.Duration {
		if len(data) > 0 {
			return data[len(data)-1]
		}
		return 0
	}), time.Millisecond)
	elapsedTimeP80 := cutoffEpsilonDuration(g, incr.Map(g, sortedElapsedTimes, func(data []time.Duration) time.Duration {
		return percentileSortedDurations(data, 80.0)
	}), time.Millisecond)
	elapsedTimeP95 := cutoffEpsilonDuration(g, incr.Map(g, sortedElapsedTimes, func(data []time.Duration) time.Duration {
		return percentileSortedDurations(data, 95.0)
	}), time.Millisecond)
	elapsedTimeP99 := cutoffEpsilonDuration(g, incr.Map(g, sortedElapsedTimes, func(data []time.Duration) time.Duration {
		return percentileSortedDurations(data, 99.0)
	}), time.Millisecond)
	maxElapsedTime := cutoffEpsilonDuration(g, incr.Map(g, sortedElapsedTimes, func(data []time.Duration) time.Duration {
		if len(data) > 0 {
			return data[len(data)-1]
		}
		return 0
	}), time.Millisecond)

	output.ElapsedMin = incr.MustObserve(g, minElapsedTime)
	output.ElapsedLast = incr.MustObserve(g, lastElapsedTime)
	output.ElapsedP80 = incr.MustObserve(g, elapsedTimeP80)
	output.ElapsedP95 = incr.MustObserve(g, elapsedTimeP95)
	output.ElapsedP99 = incr.MustObserve(g, elapsedTimeP99)
	output.ElapsedMax = incr.MustObserve(g, maxElapsedTime)

	output.TempMin, output.TempAvg, output.TempLast, output.TempMax = createStatsFor(g, window, func(a Awair) float64 {
		return a.Temp
	}, 0.5)
	output.HumidityMin, output.HumidityAvg, output.HumidityLast, output.HumidityMax = createStatsFor(g, window, func(a Awair) float64 {
		return a.Humid
	}, 0.01)
	output.PM25Min, output.PM25Avg, output.PM25Last, output.PM25Max = createStatsFor(g, window, func(a Awair) float64 {
		return a.PM25
	}, 0.5)
	output.CO2Min, output.CO2Avg, output.CO2Last, output.CO2Max = createStatsFor(g, window, func(a Awair) float64 {
		return a.CO2
	}, 1.0)

	return &output
}

func createStatsFor(g *incr.Graph, window incr.Incr[[]Awair], mapfn func(Awair) float64, epsilon float64) (min, avg, last, max incr.ObserveIncr[float64]) {
	values := incr.Map(g, window, func(data []Awair) []float64 {
		output := make([]float64, 0, len(data))
		for _, d := range data {
			output = append(output, mapfn(d))
		}
		return output
	})
	sortedValues := incr.Map(g, values, func(data []float64) []float64 {
		output := make([]float64, len(data))
		copy(output, data)
		sort.Float64s(output)
		return output
	})
	minValue := cutoffEpsilon(g, incr.Map(g, sortedValues, func(data []float64) float64 {
		if len(data) > 0 {
			return data[0]
		}
		return math.NaN()
	}), epsilon)
	avgValue := cutoffEpsilon(g, incr.Map(g, sortedValues, func(data []float64) float64 {
		if len(data) > 0 {
			var accum float64
			for _, d := range data {
				accum += d
			}
			return accum / float64(len(data))
		}
		return math.NaN()
	}), epsilon)
	lastValue := cutoffEpsilon(g, incr.Map(g, sortedValues, func(data []float64) float64 {
		if len(data) > 0 {
			return data[len(data)-1]
		}
		return math.NaN()
	}), epsilon)
	maxValue := cutoffEpsilon(g, incr.Map(g, sortedValues, func(data []float64) float64 {
		if len(data) > 0 {
			return data[len(data)-1]
		}
		return math.NaN()
	}), epsilon)
	min = incr.MustObserve(g, minValue)
	avg = incr.MustObserve(g, avgValue)
	last = incr.MustObserve(g, lastValue)
	max = incr.MustObserve(g, maxValue)
	return
}

func cutoffEpsilon(g *incr.Graph, input incr.Incr[float64], epsilon float64) incr.Incr[float64] {
	return incr.Cutoff(g, input, func(prev, next float64) bool {
		return math.Abs(prev-next) < epsilon
	})
}

func cutoffEpsilonDuration(g *incr.Graph, input incr.Incr[time.Duration], epsilon time.Duration) incr.Incr[time.Duration] {
	return incr.Cutoff(g, input, func(prev, next time.Duration) bool {
		if prev > next {
			return (prev - next) < epsilon
		}
		return (next - prev) < epsilon
	})
}

type SensorGraph struct {
	Name string

	Latest incr.VarIncr[*Awair]

	Window incr.ObserveIncr[[]Awair]

	ElapsedMin  incr.ObserveIncr[time.Duration]
	ElapsedLast incr.ObserveIncr[time.Duration]
	ElapsedP80  incr.ObserveIncr[time.Duration]
	ElapsedP95  incr.ObserveIncr[time.Duration]
	ElapsedP99  incr.ObserveIncr[time.Duration]
	ElapsedMax  incr.ObserveIncr[time.Duration]

	TempMin  incr.ObserveIncr[float64]
	TempAvg  incr.ObserveIncr[float64]
	TempLast incr.ObserveIncr[float64]
	TempMax  incr.ObserveIncr[float64]

	HumidityMin  incr.ObserveIncr[float64]
	HumidityAvg  incr.ObserveIncr[float64]
	HumidityLast incr.ObserveIncr[float64]
	HumidityMax  incr.ObserveIncr[float64]

	CO2Min  incr.ObserveIncr[float64]
	CO2Avg  incr.ObserveIncr[float64]
	CO2Last incr.ObserveIncr[float64]
	CO2Max  incr.ObserveIncr[float64]

	PM25Min  incr.ObserveIncr[float64]
	PM25Avg  incr.ObserveIncr[float64]
	PM25Last incr.ObserveIncr[float64]
	PM25Max  incr.ObserveIncr[float64]
}

func getSensorData(ctx context.Context, sensorAddresses map[string]string) (map[string]Awair, error) {
	sensorData := make(map[string]Awair)
	var resultsMu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(awairSensors))
	errors := make(chan error, len(awairSensors))
	for sensor, host := range sensorAddresses {
		go func(s, h string) {
			defer wg.Done()
			data, err := getAwairData(ctx, h)
			if err != nil {
				errors <- err
				return
			}
			resultsMu.Lock()
			sensorData[s] = data
			resultsMu.Unlock()
		}(sensor, host)
	}
	wg.Wait()
	if len(errors) > 0 {
		return nil, <-errors
	}
	return sensorData, nil
}

// Awair is the latest awair data from a sensor.
type Awair struct {
	Timestamp     time.Time     `json:"timestamp"`
	Score         float64       `json:"score"`
	DewPoint      float64       `json:"dew_point"`
	Temp          float64       `json:"temp"`
	Humid         float64       `json:"humid"`
	CO2           float64       `json:"co2"`
	VOC           float64       `json:"voc"`
	VOCBaseline   float64       `json:"voc_baseline"`
	VOCH2Raw      float64       `json:"voc_h2_raw"`
	VOCEthanolRaw float64       `json:"voc_ethanol_raw"`
	PM25          float64       `json:"pm25"`
	PM10Est       float64       `json:"pm10_est"`
	Elapsed       time.Duration `json:"-"`
}

func getAwairData(ctx context.Context, host string) (data Awair, err error) {
	const path = "/air-data/latest"
	req := http.Request{
		Method: "GET",
		URL: &url.URL{
			Scheme: "http",
			Host:   host,
			Path:   path,
		},
	}
	err = getJSON(ctx, &req, &data)
	return
}

func getJSON(ctx context.Context, req *http.Request, output *Awair) (err error) {
	started := time.Now()
	var statusCode int
	req = req.WithContext(ctx)
	var res *http.Response
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer res.Body.Close()
	if statusCode = res.StatusCode; statusCode < http.StatusOK || statusCode > 299 {
		return fmt.Errorf("non-200 returned from remote")
	}
	err = json.NewDecoder(res.Body).Decode(output)
	output.Elapsed = time.Since(started)
	return
}

const (
	queueDefaultCapacity = 4
)

// Queue is a fifo (first-in, first-out) buffer implementation.
//
// It is is backed by a pre-allocated array, which saves GC churn because the memory used
// to hold elements is not released unless the queue is trimmed.
//
// This stands in opposition to how queues are typically are implemented, which is as a linked list.
//
// As a result, `Push` can be O(n) if the backing array needs to be embiggened, though this should be relatively rare
// in pracitce if you're keeping a fixed queue size.
//
// Pop is generally O(1) because it just moves pointers around and nil's out elements.
type Queue[A any] struct {
	array []A
	head  int
	tail  int
	size  int
}

// Len returns the number of elements in the queue.
//
// Use `Cap()` to return the length of the backing array itself.
func (q *Queue[A]) Len() int {
	return q.size
}

// Cap returns the total capacity of the queue, including empty elements.
func (q *Queue[A]) Cap() int {
	return len(q.array)
}

// Clear removes all elements from the Queue.
//
// It does _not_ reclaim any backing buffer length.
//
// To resize the backing buffer, use `Trim(size)`.
func (q *Queue[A]) Clear() {
	q.head = 0
	q.tail = 0
	q.size = 0
	clear(q.array)
}

// Push adds an element to the "back" of the Queue.
func (q *Queue[A]) Push(v A) {
	if len(q.array) == 0 {
		q.array = make([]A, queueDefaultCapacity)
	} else if q.size == len(q.array) {
		q.SetCapacity(len(q.array) << 1)
	}
	q.array[q.tail] = v
	q.tail = (q.tail + 1) % len(q.array)
	q.size++
}

// Pop removes the first (oldest) element from the Queue.
func (q *Queue[A]) Pop() (output A, ok bool) {
	if q.size == 0 {
		return
	}
	var zero A
	output = q.array[q.head]
	q.array[q.head] = zero
	ok = true
	q.head = (q.head + 1) % len(q.array)
	q.size--
	return
}

// Peek returns but does not remove the first element.
func (q *Queue[A]) Peek() (output A, ok bool) {
	if q.size == 0 {
		return
	}
	output = q.array[q.head]
	ok = true
	return
}

// Values collects the storage array into a copy array which is returned.
func (q *Queue[A]) Values() (output []A) {
	if q.size == 0 {
		return
	}
	output = make([]A, 0, q.size)
	if q.head < q.tail {
		for cursor := q.head; cursor < q.tail; cursor++ {
			output = append(output, q.array[cursor])
		}
	} else {
		for cursor := q.head; cursor < len(q.array); cursor++ {
			output = append(output, q.array[cursor])
		}
		for cursor := 0; cursor < q.tail; cursor++ {
			output = append(output, q.array[cursor])
		}
	}
	return
}

// SetCapacity copies the queue into a new buffer
// with the given capacity.
//
// the new buffer will reset the head and tail
// indices such that head will be 0, and tail
// will be wherever the size index places it.
func (q *Queue[A]) SetCapacity(capacity int) {
	newArray := make([]A, capacity)
	if q.size > 0 {
		if q.head < q.tail {
			copy(newArray, q.array[q.head:q.head+q.size])
		} else {
			copy(newArray, q.array[q.head:])
			copy(newArray[len(q.array)-q.head:], q.array[:q.tail])
		}
	}
	q.array = newArray
	q.head = 0
	if capacity < q.size {
		q.size = capacity
	}
	if q.size == capacity {
		q.tail = 0
	} else {
		q.tail = q.size
	}
}

func percentileSortedDurations(sortedInput []time.Duration, percentile float64) time.Duration {
	index := (percentile / 100.0) * float64(len(sortedInput))
	if index == float64(int64(index)) {
		i := int(roundPlaces(index, 0))
		if i < 1 {
			return 0
		}

		return meanDurations([]time.Duration{sortedInput[i-1], sortedInput[i]})
	}

	i := int(roundPlaces(index, 0))
	if i < 1 {
		return time.Duration(0)
	}

	return sortedInput[i-1]
}

func roundPlaces(input float64, places int) float64 {
	if math.IsNaN(input) {
		return 0.0
	}

	sign := 1.0
	if input < 0 {
		sign = -1
		input *= -1
	}

	rounded := float64(0)
	precision := math.Pow(10, float64(places))
	digit := input * precision
	_, decimal := math.Modf(digit)

	if decimal >= 0.5 {
		rounded = math.Ceil(digit)
	} else {
		rounded = math.Floor(digit)
	}

	return rounded / precision * sign
}

func meanDurations(input []time.Duration) time.Duration {
	if len(input) == 0 {
		return 0
	}

	sum := sumDurations(input)
	mean := uint64(sum) / uint64(len(input))
	return time.Duration(mean)
}

func sumDurations(values []time.Duration) time.Duration {
	var total time.Duration
	for x := 0; x < len(values); x++ {
		total += values[x]
	}

	return total
}

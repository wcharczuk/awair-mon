package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/wcharczuk/go-diskq"
	"github.com/wcharczuk/go-incr"
)

// these may change based on DHCP settings.
var awairSensors = map[string]string{
	"Bedroom":     "192.168.4.74",
	"Office":      "192.168.4.47",
	"Living Room": "192.168.4.46",
}

func main() {
	g := incr.New()
	app := tview.NewApplication()

	logs := new(bytes.Buffer)

	table := tview.NewTable().
		SetBorders(true)

	var sensorNames []string
	for sensorName := range awairSensors {
		sensorNames = append(sensorNames, sensorName)
	}
	sort.Strings(sensorNames)

	graphs := make(map[string]*SensorGraph)
	table.SetCell(0, 0, tview.NewTableCell("Sensor"))
	table.SetCell(0, 1, tview.NewTableCell("Temp (last)"))
	table.SetCell(0, 2, tview.NewTableCell("Temp (min)"))
	table.SetCell(0, 3, tview.NewTableCell("Temp (max)"))
	table.SetCell(0, 4, tview.NewTableCell("Humidity % (last)"))
	table.SetCell(0, 5, tview.NewTableCell("CO2 (last)"))
	table.SetCell(0, 6, tview.NewTableCell("PM2.5 (last)"))
	table.SetCell(0, 7, tview.NewTableCell("Elapsed (last)"))
	table.SetCell(0, 8, tview.NewTableCell("Elapsed (P95)"))

	index := 1
	for _, sensorName := range sensorNames {
		sensorGraph := createSensorGraph(g, sensorName, 48*time.Hour /*=window_duration*/)
		graphs[sensorName] = sensorGraph
		addTableRowForSensor(sensorGraph, app, table, index)
		index++
	}

	logView := tview.NewTextView().SetText("").SetTextAlign(tview.AlignLeft)

	grid := tview.NewGrid().
		SetRows(-2, -1).
		SetColumns(0).
		SetBorders(true).
		AddItem(table, 0, 0, 1, 1, 0, 0, false).
		AddItem(logView, 1, 0, 1, 1, 0, 0, false)

	app.SetRoot(grid, true).EnableMouse(false)

	cfg := diskq.Config{
		Path:             "data",
		PartitionCount:   3,
		RetentionMaxAge:  72 * time.Hour,
		SegmentSizeBytes: 512 * 1024, // 512kb
	}

	dq, err := diskq.New(cfg)
	maybeFatal(err)
	defer dq.Close()

	go func() {
		timer := time.NewTicker(5 * time.Second)
		defer timer.Stop()
		for range timer.C {
			_ = dq.Vacuum()
		}
	}()

	go func() {
		ctx := context.Background()
		ctx = incr.WithTracingOutputs(ctx, logs, logs)
		timer := time.NewTicker(5 * time.Second)
		defer timer.Stop()
		for range timer.C {
			logs.Reset()
			data, err := getSensorDataWithTimeout(ctx, awairSensors)
			if err != nil {
				incr.TraceErrorf(ctx, "error getting sensor data: %v", err)
				continue
			}
			for sensor, result := range data {
				dq.Push(diskq.Message{PartitionKey: sensor, Data: encodeResult(result)})
				incr.TracePrintf(ctx, "fetched %s in %v", sensor, result.Elapsed.Round(time.Millisecond))
				graphs[sensor].Latest.Set(result)
			}
			if err = g.ParallelStabilize(ctx); err != nil {
				incr.TraceErrorf(ctx, "stabilization error: %v", err)
			}
			app.QueueUpdate(func() {
				logView.SetText(logs.String())
			})
		}
	}()

	err = app.Run()
	if err != nil {
		maybeFatal(err)
	}
}

func encodeResult(v any) []byte {
	data, _ := json.Marshal(v)
	return data
}

func tempRange(temp float64) tcell.Color {
	if temp > 30 {
		return tcell.ColorRed
	}
	if temp > 22 {
		return tcell.ColorYellow
	}
	if temp < 18 {
		return tcell.ColorBlue
	}
	return tcell.ColorWhite
}

func co2Range(co2 int) tcell.Color {
	if co2 > 1500 {
		return tcell.ColorRed
	}
	if co2 > 800 {
		return tcell.ColorYellow
	}
	return tcell.ColorWhite
}

func humidRange(humid float64) tcell.Color {
	if humid > 80.0 {
		return tcell.ColorRed
	}
	if humid > 60.0 {
		return tcell.ColorYellow
	}
	return tcell.ColorWhite
}

func pm25Range(pm25 int) tcell.Color {
	if pm25 > 40 {
		return tcell.ColorRed
	}
	if pm25 > 20 {
		return tcell.ColorYellow
	}
	return tcell.ColorWhite
}

func elapsedRange(elapsed time.Duration) tcell.Color {
	if elapsed > 300*time.Millisecond {
		return tcell.ColorRed
	}
	if elapsed > 100*time.Millisecond {
		return tcell.ColorYellow
	}
	return tcell.ColorWhite
}

func defaultRange[A any](v A) tcell.Color {
	return tcell.ColorWhite
}

func updateCell[A any](app *tview.Application, cell *tview.TableCell, colorRange func(A) tcell.Color, newTextFormat string, value A) {
	app.QueueUpdateDraw(func() {
		cell.SetBackgroundColor(tcell.ColorWhiteSmoke)
		newColor := colorRange(value)
		if newColor == tcell.ColorWhite {
			newColor = tcell.ColorBlack
		}
		cell.SetTextColor(newColor)
		cell.SetText(fmt.Sprintf(newTextFormat, value))
	})
	go func() {
		time.Sleep(2 * time.Second)
		app.QueueUpdateDraw(func() {
			if cell.Color == tcell.ColorBlack {
				cell.SetTextColor(tcell.ColorWhite)
			}
			cell.SetTransparency(true)
		})
	}()
}

func addTableRowForSensor(sensorGraph *SensorGraph, app *tview.Application, table *tview.Table, index int) {
	labelCell := tview.NewTableCell(sensorGraph.Name).SetTextColor(tcell.ColorWhite).SetAlign(tview.AlignRight)
	table.SetCell(index, 0, labelCell)

	tempLastCell := tview.NewTableCell("").SetTextColor(tcell.ColorWhite).SetAlign(tview.AlignCenter)
	sensorGraph.TempLast.OnUpdate(func(_ context.Context, temp float64) {
		updateCell(app, tempLastCell, tempRange, "%0.2fc", temp)
	})
	table.SetCell(index, 1, tempLastCell)

	tempMinCell := tview.NewTableCell("").SetTextColor(tcell.ColorWhite).SetAlign(tview.AlignCenter)
	sensorGraph.TempMin.OnUpdate(func(_ context.Context, temp float64) {
		updateCell(app, tempMinCell, tempRange, "%0.2fc", temp)
	})
	table.SetCell(index, 2, tempMinCell)

	tempMaxCell := tview.NewTableCell("").SetTextColor(tcell.ColorWhite).SetAlign(tview.AlignCenter)
	sensorGraph.TempMax.OnUpdate(func(_ context.Context, temp float64) {
		updateCell(app, tempMaxCell, tempRange, "%0.2fc", temp)
	})
	table.SetCell(index, 3, tempMaxCell)

	humidCell := tview.NewTableCell("").SetTextColor(tcell.ColorWhite).SetAlign(tview.AlignCenter)
	sensorGraph.HumidityLast.OnUpdate(func(_ context.Context, humidity float64) {
		updateCell(app, humidCell, humidRange, "%0.2f%%", humidity)
	})
	table.SetCell(index, 4, humidCell)

	co2Cell := tview.NewTableCell("").SetTextColor(tcell.ColorWhite).SetAlign(tview.AlignCenter)
	sensorGraph.CO2Last.OnUpdate(func(_ context.Context, co2 float64) {
		updateCell(app, co2Cell, co2Range, "%d", int(co2))
	})
	table.SetCell(index, 5, co2Cell)

	pm25Cell := tview.NewTableCell("").SetTextColor(tcell.ColorWhite).SetAlign(tview.AlignCenter)
	sensorGraph.PM25Last.OnUpdate(func(_ context.Context, pm25 float64) {
		updateCell(app, pm25Cell, pm25Range, "%d", int(pm25))
	})
	table.SetCell(index, 6, pm25Cell)

	elapsedCell := tview.NewTableCell("").SetTextColor(tcell.ColorWhite).SetAlign(tview.AlignCenter)
	sensorGraph.ElapsedLast.OnUpdate(func(_ context.Context, elapsed time.Duration) {
		updateCell(app, elapsedCell, elapsedRange, "%v", elapsed.Round(time.Millisecond))
	})
	table.SetCell(index, 7, elapsedCell)

	elapsedP95Cell := tview.NewTableCell("").SetTextColor(tcell.ColorWhite).SetAlign(tview.AlignCenter)
	sensorGraph.ElapsedP95.OnUpdate(func(_ context.Context, elapsed time.Duration) {
		updateCell(app, elapsedP95Cell, elapsedRange, "%v", elapsed.Round(time.Millisecond))
	})
	table.SetCell(index, 8, elapsedP95Cell)
}

func maybeFatal(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}

func createSensorGraph(g *incr.Graph, name string, windowLength time.Duration) *SensorGraph {
	output := SensorGraph{
		Name: name,
	}
	output.Latest = incr.Var[Awair](g, Awair{})
	windowData := new(Queue[Awair])
	window := incr.Map(g, output.Latest, func(sample Awair) []Awair {
		if sample.Timestamp.IsZero() {
			return nil
		}
		windowData.Push(sample)
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
	}, 0.1)
	output.PM25Min, output.PM25Avg, output.PM25Last, output.PM25Max = createStatsFor(g, window, func(a Awair) float64 {
		return a.PM25
	}, 1.0)
	output.CO2Min, output.CO2Avg, output.CO2Last, output.CO2Max = createStatsFor(g, window, func(a Awair) float64 {
		return a.CO2
	}, 5.0)

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
		return 0
	}), epsilon)
	avgValue := cutoffEpsilon(g, incr.Map(g, sortedValues, func(data []float64) float64 {
		if len(data) > 0 {
			var accum float64
			for _, d := range data {
				accum += d
			}
			return accum / float64(len(data))
		}
		return 0
	}), epsilon)
	lastValue := cutoffEpsilon(g, incr.Map(g, values, func(data []float64) float64 {
		if len(data) > 0 {
			return data[len(data)-1]
		}
		return 0
	}), epsilon)
	maxValue := cutoffEpsilon(g, incr.Map(g, sortedValues, func(data []float64) float64 {
		if len(data) > 0 {
			return data[len(data)-1]
		}
		return 0
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

	Latest incr.VarIncr[Awair]

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

func getSensorDataWithTimeout(ctx context.Context, sensorAddresses map[string]string) (map[string]Awair, error) {
	timeoutctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return getSensorData(timeoutctx, sensorAddresses)
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
			data.Sensor = s
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

type Awair struct {
	Sensor        string        `json:"sensor"`
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
	incr.TracePrintf(ctx, "fetching sensor data from: %s", req.URL.String())
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

type Queue[A any] struct {
	array []A
	head  int
	tail  int
	size  int
}

func (q *Queue[A]) Len() int {
	return q.size
}

func (q *Queue[A]) Cap() int {
	return len(q.array)
}

func (q *Queue[A]) Clear() {
	q.head = 0
	q.tail = 0
	q.size = 0
	clear(q.array)
}

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

func (q *Queue[A]) Peek() (output A, ok bool) {
	if q.size == 0 {
		return
	}
	output = q.array[q.head]
	ok = true
	return
}

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

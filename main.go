package main

import (
	"fmt"
	"golangchallenge/processors"
	"math/rand"
	"runtime"
	"sync"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
)

func GetTripsData(wg *sync.WaitGroup) *processors.TripsData {
	data := &processors.TripsData{}
	// Generate 10000 drivers
	data.Drivers = make([]*processors.Driver, 0)
	for i := 0; i < 2; i++ {
		data.Drivers = append(data.Drivers, &processors.Driver{
			Id:   uuid.NewString(),
			Name: gofakeit.Name(),
		})
	}
	// Generate 100 hotels
	data.Hotels = make([]*processors.Hotel, 0)
	for i := 0; i < 2; i++ {
		data.Hotels = append(data.Hotels, &processors.Hotel{
			Id:   uuid.NewString(),
			Name: gofakeit.City(),
		})
	}
	// Create a channel for trips with 1000 buffer
	data.Trips = make(chan *processors.Trip, 1000)
	go func() {
		wg.Add(1)
		// Generate 10000000 trips
		for i := 0; i < 10; i++ {
			driver := data.Drivers[rand.Intn(len(data.Drivers))]
			hotel := data.Hotels[rand.Intn(len(data.Hotels))]
			data.Trips <- &processors.Trip{
				Id:           uuid.NewString(),
				DriverId:     driver.Id,
				HotelId:      hotel.Id,
				DriverRating: float64(rand.Intn(6)),
				HotelRating:  float64(rand.Intn(6)),
				Status:       "complete",
				Driver:       driver,
				Hotel:        hotel,
			}
		}
		close(data.Trips)
		wg.Done()
	}()
	return data
}

func main() {
	var m1, m2 runtime.MemStats
	runtime.GC()
    runtime.ReadMemStats(&m1)
	wg := &sync.WaitGroup{}
	
	data := GetTripsData(wg)
	processor := processors.CreateProcessorFromData(data, wg)
	err := processor.StartProcessing()
	if err != nil {
		fmt.Printf("Error while processing data: %s\n", err)
		return
	}
	// Wait for processor to finish processing data
	wg.Wait()
	topDriver := processor.GetTopRankedDriver()
	fmt.Printf("Top driver found: %s\n", topDriver)
	topHotel := processor.GetTopRankedHotel()
	fmt.Printf("Top hotel found: %s\n", topHotel)
	runtime.ReadMemStats(&m2)
    fmt.Println("total:", m2.TotalAlloc - m1.TotalAlloc)
    fmt.Println("mallocs:", m2.Mallocs - m1.Mallocs)
}

package processors

import (
	"sync"
)

type DriverRanking struct {
	AverageRating float64
	TotalRating   float64
	TotalTrips    int
	DriverId      string
	DriverName    string
}

func (d *DriverRanking) String() string {
	// Implement this function
	return d.DriverName
}

type HotelRanking struct {
	AverageRating float64
	TotalRating   float64
	NoOfTrips     int
	HotelId       string
	HotelName     string
}

func (h *HotelRanking) String() string {
	// Implement this function
	return h.HotelName
}

type ProcessorInterface interface {
	StartProcessing() error
	GetTopRankedDriver() *DriverRanking
	GetTopRankedHotel() *HotelRanking
}

func CreateProcessorFromData(data *TripsData, wg *sync.WaitGroup) ProcessorInterface {
	// @todo Initialize your processor here

	return NewProcessor(data, wg, make(DriverResults), make(HotelResults))
}

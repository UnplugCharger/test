package processors

import (
	"log"
	"sync"
)

// Processor should implement ProcessorInterface
// Hint: Try using goroutines to process the data in parallel
// Requirement: Do not store all Trips in memory and cache your results
type Processor struct {
	data          *TripsData
	wg            *sync.WaitGroup
	DriverRanking DriverResults
	HotelRanking  HotelResults
}

var mux sync.RWMutex

type DriverResults map[string]*DriverRanking
type HotelResults map[string]*HotelRanking

func (d DriverResults) AddOrUpdate(driverId string, rank *DriverRanking) {
	mux.Lock()
	defer mux.Unlock()
	d[driverId] = rank
}

func (h HotelResults) AddOrUpdate(hotelId string, rank *HotelRanking) {
	mux.Lock()
	defer mux.Unlock()
	h[hotelId] = rank
}

// GetDriverByKey  driver by key
func (d DriverResults) GetDriverByKey(driverId string) *DriverRanking {
	currentDriver, ok := d[driverId]
	if !ok {
		return nil
	}
	return currentDriver

}

// GetHotelByKey  hotel by key
func (h HotelResults) GetHotelByKey(hotelId string) *HotelRanking {
	currentHotel, ok := h[hotelId]
	if !ok {
		return nil
	}
	return currentHotel

}

// PeekHotelMax get the highest ranked hotel so far
func (h HotelResults) PeekHotelMax() *HotelRanking {
	var highestRatedSoFar *HotelRanking
	for _, v := range h {
		if highestRatedSoFar == nil {
			highestRatedSoFar = v
		}
		if v.AverageRating > highestRatedSoFar.AverageRating {
			highestRatedSoFar = v
		}
	}
	return highestRatedSoFar
}

// PeekDriverMax PeekMax Check the  driver with max trips at the moment
func (d DriverResults) PeekDriverMax() *DriverRanking {
	var largestSoFar *DriverRanking
	for _, v := range d {
		if largestSoFar == nil {
			largestSoFar = v
		}
		if v.AverageRating > largestSoFar.AverageRating {
			largestSoFar = v
		}

	}
	return largestSoFar

}

//NewProcessor creates an instance of the processor
func NewProcessor(data *TripsData, wg *sync.WaitGroup,
	dr DriverResults, hotelRanking HotelResults) *Processor {
	return &Processor{
		data:          data,
		wg:            wg,
		DriverRanking: dr,
		HotelRanking:  hotelRanking,
	}
}

func (p *Processor) StartProcessing() error {
	// My go routines should go here, but I kept on getting nil pointer error
	for trip := range p.data.Trips {
		p.processDriverRanking(trip)
		p.processHotelRanking(trip)
	}

	return nil
}

func (p *Processor) processDriverRanking(trip *Trip) {
	// @todo Implement this function
	driverId := trip.DriverId
	driverRanking := p.DriverRanking.GetDriverByKey(driverId)

	if driverRanking == nil {
		driverRanking := &DriverRanking{
			AverageRating: trip.DriverRating,
			TotalRating:   trip.DriverRating,
			TotalTrips:    1,
			DriverId:      driverId,
			DriverName:    trip.Driver.Name,
		}
		p.DriverRanking.AddOrUpdate(driverId, driverRanking)
		return
	}

	driverRanking.TotalRating += trip.DriverRating
	driverRanking.TotalTrips += 1
	driverRanking.AverageRating = driverRanking.TotalRating / float64(driverRanking.TotalTrips)

	p.DriverRanking.AddOrUpdate(driverId, driverRanking)
}

func (p *Processor) GetTopRankedDriver() *DriverRanking {
	allDrivers := len(p.DriverRanking)
	log.Println("All drivers: ", allDrivers)
	topRankNode := p.DriverRanking.PeekDriverMax()

	if topRankNode == nil {
		return nil
	}

	return topRankNode

}

func (p *Processor) processHotelRanking(trip *Trip) {
	hotelId := trip.HotelId
	hotelRanking := p.HotelRanking.GetHotelByKey(hotelId)

	if hotelRanking == nil {
		hotelRanking := &HotelRanking{
			AverageRating: trip.HotelRating,
			TotalRating:   trip.HotelRating,
			NoOfTrips:     1,
			HotelId:       hotelId,
			HotelName:     trip.Hotel.Name,
		}
		p.HotelRanking.AddOrUpdate(hotelId, hotelRanking)
		return
	}

	hotelRanking.TotalRating += trip.HotelRating
	hotelRanking.NoOfTrips += 1
	hotelRanking.AverageRating = hotelRanking.TotalRating / float64(hotelRanking.NoOfTrips)

	p.HotelRanking.AddOrUpdate(hotelId, hotelRanking)
}

func (p *Processor) GetTopRankedHotel() *HotelRanking {
	topRankNode := p.HotelRanking.PeekHotelMax()

	if topRankNode == nil {
		return nil
	}

	return topRankNode
}

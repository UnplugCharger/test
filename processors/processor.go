package processors

import (
	"sync"

	"github.com/wangjia184/sortedset"
)

// Processor should implement ProcessorInterface
// Hint: Try using goroutines to process the data in parallel
// Requirement: Do not store all Trips in memory and cache your results
type Processor struct {
   data *TripsData
   wg *sync.WaitGroup
   DriverRanking *sortedset.SortedSet
   HotelRanking *sortedset.SortedSet


   
}

func NewProcesor (data *TripsData, wg *sync.WaitGroup,
	driverRanking *sortedset.SortedSet , hotelRanking *sortedset.SortedSet) *Processor {
   return &Processor{
	  data: data,
	  wg: wg,
	  DriverRanking: driverRanking,
	  HotelRanking: hotelRanking,
   }
}


func (p *Processor) StartProcessing() error {
     // My go routines should go here  but I kept on getting nill pointer error
	for trip := range p.data.Trips {
		p.processDriverRanking(trip)
		p.processHotelRanking(trip)
	}

	

	return nil 
}

func (p *Processor) processDriverRanking(trip *Trip) {
   // @todo Implement this function
   driverId := trip.DriverId
   ranking := p.DriverRanking.GetByKey(driverId)
    
   if ranking == nil {
	  driverRanking := &DriverRanking{
		 AverageRating: trip.DriverRating,
		 TotalRating: trip.DriverRating,
		 TotalTrips: 1,
		 DriverId: driverId,
		 DriverName: trip.Driver.Name,
	  }
	  p.DriverRanking.AddOrUpdate(driverId,sortedset.SCORE(trip.DriverRating),driverRanking)
	  return
   }
   driverRanking := ranking.Value.(*DriverRanking)
   driverRanking.TotalRating += trip.DriverRating
   driverRanking.TotalTrips += 1
   driverRanking.AverageRating = driverRanking.TotalRating / float64(driverRanking.TotalTrips)

   p.DriverRanking.AddOrUpdate(driverId,sortedset.SCORE(driverRanking.AverageRating),driverRanking)
}

func (p *Processor) GetTopRankedDriver() *DriverRanking {
	topRankNode := p.DriverRanking.PeekMax()

	if topRankNode == nil {
		return nil
	}
	topDriver := topRankNode.Value.(*DriverRanking)

	return topDriver

}

func (p *Processor) processHotelRanking(trip *Trip) {
	hotelId := trip.HotelId
	ranking := p.HotelRanking.GetByKey(hotelId)

	if ranking == nil {
	   hotelRanking := &HotelRanking{
		  AverageRating: trip.HotelRating,
		  TotalRating: trip.HotelRating,
		  NoOfTrips: 1,
		  HotelId: hotelId,
		  HotelName: trip.Hotel.Name,
	   }
	   p.HotelRanking.AddOrUpdate(hotelId,sortedset.SCORE(trip.HotelRating),hotelRanking)
	   return
	}
	hotelRanking := ranking.Value.(*HotelRanking)
	hotelRanking.TotalRating += trip.HotelRating
	hotelRanking.NoOfTrips += 1
	hotelRanking.AverageRating = hotelRanking.TotalRating / float64(hotelRanking.NoOfTrips)

	p.HotelRanking.AddOrUpdate(hotelId,sortedset.SCORE(hotelRanking.AverageRating),hotelRanking)
}


func (p *Processor) GetTopRankedHotel() *HotelRanking {
	topRankNode := p.HotelRanking.PeekMax()

	if topRankNode == nil {
		return nil
	}
	topHotel := topRankNode.Value.(*HotelRanking)

	return topHotel
}
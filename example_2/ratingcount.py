from mrjob.job import MRJob

class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet, UContinent, ReviewMonth, ReviewDay) = line.split("\t")
    
        yield (HName, (raiting, 1))

    def _reducer_combiner(self, HName, raiting):
      avg, count = 0, 0
      for tmp, c in raiting:
        avg = (avg * count + tmp * c) / (count + c)
        count += c
      return (HName, (avg, count))

    def combiner(self, HName, raiting):
      yield self._reducer_combiner(HName, raiting)
      
    def reducer(self, HName, raiting):
      HName, (avg, count) = self._reducer_combiner(HName, raiting)
      yield (month, avg)


if __name__ == '__main__':
    MRHotelRaitingCount.run()

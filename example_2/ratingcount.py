from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet, UContinent, ReviewMonth, ReviewDay) = line.split("\t")
        wynik = [rating, 1]
        yield (HName, wynik)

    def _reducer_combiner(self, HName, rating):
      avg, count = 0, 0
      for tmp, c in rating:
        avg = (avg * count + tmp * c) / (count + c)
        count += c
      return (HName, (avg, count))

    def combiner(self, HName, rating):
      yield self._reducer_combiner(HName, rating)
      
    def reducer(self, HName, rating):
      HName, (avg, count) = self._reducer_combiner(HName, rating)
      yield (HName, avg)


if __name__ == '__main__':
    MRHotelRaitingCount.run()

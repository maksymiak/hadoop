from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet,
         UContinent, ReviewMonth, ReviewDay) = line.split("\t")

        yield HName, rating

    def _reducer_combiner(self, HName, ratings):
        avg, count = 0, 0
        for tmp, c in ratings:
            avg = (avg * count + tmp * c) / (count + c)
            count += c
        return (HName, (avg, count))


    def combiner(self, HName, ratings):
        yield self._reducer_combiner(HName, ratings)


    def reducer(self, HName, ratings):
        HName, (avg, count) = self._reducer_combiner(HName, ratings)

        yield (HName, rating)

if _name_ == '__main__':
    MRHotelRaitingCount.run()

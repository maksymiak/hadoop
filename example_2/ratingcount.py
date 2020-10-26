from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet,
         UContinent, ReviewMonth, ReviewDay) = line.split("\t")

        result = [HName, rating]
        yield result


    def reducer(self, key, ratings):
        avg, count = 0, 0
        for tmp, c in ratings:
            avg = (avg * count + tmp * c) / (count + c)
            count += c
        result = (key, (avg, count))

        yield result

if __name__ == '__main__':
    MRHotelRaitingCount.run()

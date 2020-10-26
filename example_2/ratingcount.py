from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet, UContinent, ReviewMonth, ReviewDay) = line.split("\t")
    
        yield (HName, (rating, 1))

if __name__ == '__main__':
    MRHotelRaitingCount.run()

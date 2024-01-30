from datetime import datetime
import time


class Event:
    def __init__(self, name, event_date, scrapping_start_date, scrapping_end_date):
        self.name = name
        self.event_date = event_date
        self.scrapping_start_date = scrapping_start_date
        self.scrapping_end_date = scrapping_end_date
        self.date_format = "%Y-%m-%d"

    def get_unix_time(self, human_time):
        date = datetime.strptime(human_time, self.date_format)
        return int(time.mktime(date.timetuple()))

    def get_event_date_unix(self):
        return self.get_unix_time(self.event_date)

    def get_scrapping_start_date_unix(self):
        return self.get_unix_time(self.scrapping_start_date)

    def get_scrapping_end_date_unix(self):
        return self.get_unix_time(self.scrapping_end_date)


events = [
    Event("2008_Recession", "2007-12-01", "2007-10-01", "2008-02-01"),
    Event("Boston_Marathon_bombing_2013", "2013-04-15", "2013-02-15", "2013-06-15")
]

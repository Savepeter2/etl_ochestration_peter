from typing import Optional

from sqlalchemy import Column, DateTime, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.sql import func

Base = declarative_base()


class Weather(Base):

    __tablename__ = "weather"

    id: int = Column(Integer, primary_key=True, index=True)
    city: str = Column(String(50), nullable=False)
    country: str = Column(String(50), nullable=False)
    state: str = Column(String(50), nullable=False)
    latitude: float = Column(Float(10), nullable=False)
    longitude: float = Column(Float(10), nullable=False)
    timezone: str = Column(String(50), nullable=False)
    timezone_offset: int = Column(Integer, nullable=False)
    date_time: DateTime = Column(DateTime(timezone=True), nullable=False)
    sunrise: int = Column(Integer, nullable=False)
    sunset: int = Column(Integer, nullable=False)
    temperature: int = Column(Integer, nullable=False)
    feels_like: float = Column(Float(10), nullable=False)
    pressure: int = Column(Integer, nullable=False)
    humidity: int = Column(Integer, nullable=False)
    dew_point: float = Column(Float(10), nullable=False)
    ultraviolet_index: float = Column(Float(10), nullable=False)
    clouds: int = Column(Integer, nullable=False)
    visibility: int = Column(Integer, nullable=False)
    wind_speed: float = Column(Float(10), nullable=False)
    wind_deg: int = Column(Integer, nullable=False)
    weather: str = Column(String(50), nullable=False)
    description: str = Column(String(50), nullable=False)
    created_at: DateTime = Column(DateTime(timezone=True), server_default=func.now())
    updated_at: DateTime = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return (
            f"Weather(city={self.city}, country={self.country}, state={self.state}, "
            f"latitude={self.latitude}, longitude={self.longitude}, timezone={self.timezone}, "
            f"timezone_offset={self.timezone_offset}, date_time={self.date_time}, "
            f"sunrise={self.sunrise}, sunset={self.sunset}, temperature={self.temperature}, "
            f"feels_like={self.feels_like}, pressure={self.pressure}, humidity={self.humidity}, "
            f"dew_point={self.dew_point}, ultraviolet_index={self.ultraviolet_index}, "
            f"clouds={self.clouds}, visibility={self.visibility}, wind_speed={self.wind_speed}, "
            f"wind_deg={self.wind_deg}, weather={self.weather}, description={self.description}, "
            f"created_at={self.created_at}, updated_at={self.updated_at}"
        )

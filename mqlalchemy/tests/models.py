## -*- coding: utf-8 -*-\
"""
    mqlalchemy.tests.models.py
    ~~~~~

    SQLAlchemy models for the Chinook database.

    :copyright: (c) 2015 by Nicholas Repole.
    :license: BSD - See LICENSE for more details.
"""
from sqlalchemy import Column, DateTime, ForeignKey, Integer, Numeric, \
    Table, Unicode
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class Album(Base):

    """SQLAlchemy model for the Album table in our database."""

    __tablename__ = 'Album'

    AlbumId = Column(Integer, primary_key=True)
    Title = Column(Unicode(160), nullable=False)
    ArtistId = Column(
        ForeignKey('Artist.ArtistId'), nullable=False, index=True)

    Artist = relationship('Artist')


class Artist(Base):

    """SQLAlchemy model for the Artist table in our database."""

    __tablename__ = 'Artist'

    ArtistId = Column(Integer, primary_key=True)
    Name = Column(Unicode(120))


class Customer(Base):

    """SQLAlchemy model for the Customer table in our database."""

    __tablename__ = 'Customer'

    CustomerId = Column(Integer, primary_key=True)
    FirstName = Column(Unicode(40), nullable=False)
    LastName = Column(Unicode(20), nullable=False)
    Company = Column(Unicode(80))
    Address = Column(Unicode(70))
    City = Column(Unicode(40))
    State = Column(Unicode(40))
    Country = Column(Unicode(40))
    PostalCode = Column(Unicode(10))
    Phone = Column(Unicode(24))
    Fax = Column(Unicode(24))
    Email = Column(Unicode(60), nullable=False)
    SupportRepId = Column(ForeignKey('Employee.EmployeeId'), index=True)

    Employee = relationship('Employee')


class Employee(Base):

    """SQLAlchemy model for the Employee table in our database."""

    __tablename__ = 'Employee'

    EmployeeId = Column(Integer, primary_key=True)
    LastName = Column(Unicode(20), nullable=False)
    FirstName = Column(Unicode(20), nullable=False)
    Title = Column(Unicode(30))
    ReportsTo = Column(ForeignKey('Employee.EmployeeId'), index=True)
    BirthDate = Column(DateTime)
    HireDate = Column(DateTime)
    Address = Column(Unicode(70))
    City = Column(Unicode(40))
    State = Column(Unicode(40))
    Country = Column(Unicode(40))
    PostalCode = Column(Unicode(10))
    Phone = Column(Unicode(24))
    Fax = Column(Unicode(24))
    Email = Column(Unicode(60))

    parent = relationship('Employee', remote_side=[EmployeeId])


class Genre(Base):

    """SQLAlchemy model for the Genre table in our database."""

    __tablename__ = 'Genre'

    GenreId = Column(Integer, primary_key=True)
    Name = Column(Unicode(120))


class Invoice(Base):

    """SQLAlchemy model for the Invoice table in our database."""

    __tablename__ = 'Invoice'

    InvoiceId = Column(Integer, primary_key=True)
    CustomerId = Column(
        ForeignKey('Customer.CustomerId'), nullable=False, index=True)
    InvoiceDate = Column(DateTime, nullable=False)
    BillingAddress = Column(Unicode(70))
    BillingCity = Column(Unicode(40))
    BillingState = Column(Unicode(40))
    BillingCountry = Column(Unicode(40))
    BillingPostalCode = Column(Unicode(10))
    Total = Column(Numeric(10, 2), nullable=False)

    Customer = relationship('Customer')


class InvoiceLine(Base):

    """SQLAlchemy model for the InvoiceLine table in our database."""

    __tablename__ = 'InvoiceLine'

    InvoiceLineId = Column(Integer, primary_key=True)
    InvoiceId = Column(
        ForeignKey('Invoice.InvoiceId'), nullable=False, index=True)
    TrackId = Column(ForeignKey('Track.TrackId'), nullable=False, index=True)
    UnitPrice = Column(Numeric(10, 2), nullable=False)
    Quantity = Column(Integer, nullable=False)

    Invoice = relationship('Invoice')
    Track = relationship('Track')


class MediaType(Base):

    """SQLAlchemy model for the MediaType table in our database."""

    __tablename__ = 'MediaType'

    MediaTypeId = Column(Integer, primary_key=True)
    Name = Column(Unicode(120))


class Playlist(Base):

    """SQLAlchemy model for the Playlist table in our database."""

    __tablename__ = 'Playlist'

    PlaylistId = Column(Integer, primary_key=True)
    Name = Column(Unicode(120))

    Track = relationship('Track', secondary='PlaylistTrack')


t_PlaylistTrack = Table(
    'PlaylistTrack', metadata,
    Column(
        'PlaylistId',
        ForeignKey('Playlist.PlaylistId'),
        primary_key=True,
        nullable=False),
    Column(
        'TrackId',
        ForeignKey('Track.TrackId'),
        primary_key=True,
        nullable=False,
        index=True)
)


class Track(Base):

    """SQLAlchemy model for the Track table in our database."""

    __tablename__ = 'Track'

    TrackId = Column(Integer, primary_key=True)
    Name = Column(Unicode(200), nullable=False)
    AlbumId = Column(ForeignKey('Album.AlbumId'), index=True)
    MediaTypeId = Column(
        ForeignKey('MediaType.MediaTypeId'), nullable=False, index=True)
    GenreId = Column(ForeignKey('Genre.GenreId'), index=True)
    Composer = Column(Unicode(220))
    Milliseconds = Column(Integer, nullable=False)
    Bytes = Column(Integer)
    UnitPrice = Column(Numeric(10, 2), nullable=False)

    Album = relationship('Album')
    Genre = relationship('Genre')
    MediaType = relationship('MediaType')


t_sqlite_sequence = Table(
    'sqlite_sequence', metadata,
    Column('name', NullType),
    Column('seq', NullType)
)

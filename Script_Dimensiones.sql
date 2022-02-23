--dim_customer
CREATE TABLE dim_customer (
    CustomerId   INTEGER       PRIMARY KEY AUTOINCREMENT
                               NOT NULL,
    FirstName    NVARCHAR (40) NOT NULL,
    LastName     NVARCHAR (20) NOT NULL,
    Company      NVARCHAR (80),
    Address      NVARCHAR (70),
    City         NVARCHAR (40),
    State        NVARCHAR (40),
    Country      NVARCHAR (40),
    PostalCode   NVARCHAR (10),
    Phone        NVARCHAR (24),
    Fax          NVARCHAR (24),
    Email        NVARCHAR (60) NOT NULL);
    
--dim_invoice_items
CREATE TABLE dim_invoice_items (
    InvoiceLineId INTEGER         PRIMARY KEY AUTOINCREMENT
                                  NOT NULL,
    UnitPrice     NUMERIC (10, 2) NOT NULL,
    Quantity      INTEGER         NOT NULL
);
    
--dim_genres
CREATE TABLE dim_genres (
    GenreId INTEGER        PRIMARY KEY AUTOINCREMENT
                           NOT NULL,
    Name_Genre    NVARCHAR (120) 
);

--dim_playlist
CREATE TABLE dim_playlist (
    PlaylistId INTEGER        PRIMARY KEY AUTOINCREMENT
                              NOT NULL,
    Name_Playlist       NVARCHAR (120) 
);

--dim_albums
CREATE TABLE dim_albums (
    AlbumId  INTEGER        PRIMARY KEY AUTOINCREMENT
                            NOT NULL,
    Title_Album    NVARCHAR (160) NOT NULL
);

--dim_artist
CREATE TABLE dim_artists (
    ArtistId INTEGER        PRIMARY KEY AUTOINCREMENT
                            NOT NULL,
    Name_Artist     NVARCHAR (120) 
);

--dim_employee
CREATE TABLE dim_employee (
    EmployeeId INTEGER       PRIMARY KEY AUTOINCREMENT
                             NOT NULL,
    LastName   NVARCHAR (20) NOT NULL,
    FirstName  NVARCHAR (20) NOT NULL,
    Title      NVARCHAR (30),
    BirthDate  DATETIME,
    HireDate   DATETIME,
    Address    NVARCHAR (70),
    City       NVARCHAR (40),
    State      NVARCHAR (40),
    Country    NVARCHAR (40),
    PostalCode NVARCHAR (10),
    Phone      NVARCHAR (24),
    Fax        NVARCHAR (24),
    Email      NVARCHAR (60)
);

--dim_track
CREATE TABLE tracks (
    TrackId      INTEGER         PRIMARY KEY AUTOINCREMENT
                                 NOT NULL,
    Name_Track        NVARCHAR (200)  NOT NULL,
    Composer     NVARCHAR (220),
    Milliseconds INTEGER         NOT NULL,
    Bytes        INTEGER,
    UnitPrice    NUMERIC (10, 2) NOT NULL
);

--dim_location
CREATE TABLE dim_location (
    LocationId         INTEGER         PRIMARY KEY AUTOINCREMENT
                                      NOT NULL,
    BillingAddress    NVARCHAR (70),
    BillingCity       NVARCHAR (40),
    BillingCountry    NVARCHAR (40)
);

--dim_time
CREATE TABLE dim_time (
    TimeId         INTEGER         PRIMARY KEY AUTOINCREMENT
                                      NOT NULL
);
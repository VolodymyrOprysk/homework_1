CREATE TABLE IF NOT EXISTS Genders (
    GenderID INT PRIMARY KEY AUTO_INCREMENT,
    Gender VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Locations (
    LocationID INT PRIMARY KEY AUTO_INCREMENT,
    Location VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Interests (
    InterestID INT PRIMARY KEY AUTO_INCREMENT,
    Interest VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Users (
    UserID INT PRIMARY KEY AUTO_INCREMENT,
    Age INT,
    GenderID INT,
    FOREIGN KEY (GenderID) REFERENCES Genders(GenderID),
    LocationID INT,
    FOREIGN KEY (LocationID) REFERENCES Locations(LocationID),  
    SignupDate DATE
);

CREATE TABLE IF NOT EXISTS UsersInterests (
    UserInterestID INT PRIMARY KEY AUTO_INCREMENT,
    UserID INT,
    FOREIGN KEY (UserID) REFERENCES Users(UserID),
    InterestID INT,
    FOREIGN KEY (InterestID) REFERENCES Interests(InterestID)
);

CREATE TABLE IF NOT EXISTS Advertisers (
    AdvertiserID INT PRIMARY KEY AUTO_INCREMENT,
    Advertiser VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS AdSlotSizes (
    AdSlotSizeID INT PRIMARY KEY AUTO_INCREMENT,
    AdSlotWidth INT NOT NULL,
    AdSlotHeight INT NOT NULL
);

CREATE TABLE IF NOT EXISTS Campaigns (
    CampaignID INT PRIMARY KEY,
    CampaignName VARCHAR(255) NOT NULL,
    AdvertiserID INT NOT NULL,
    CampaignStartDate DATE NOT NULL,
    CampaignEndDate DATE NOT NULL,
    AdSlotSizeID INT NOT NULL,
    TargetAgeStart INT NOT NULL,
    TargetAgeEnd INT NOT NULL,
    Budget DECIMAL(15, 2),
    RemainingBudget DECIMAL(15, 2),
    FOREIGN KEY (AdvertiserID) REFERENCES Advertisers(AdvertiserID),
    FOREIGN KEY (AdSlotSizeID) REFERENCES AdSlotSizes(AdSlotSizeID)
);


CREATE TABLE IF NOT EXISTS CampaignsTargetingInterests (
    CampaignTargetInterestID INT PRIMARY KEY AUTO_INCREMENT,
    CampaignID INT NOT NULL,
    InterestID INT NOT NULL,
    FOREIGN KEY (CampaignID) REFERENCES Campaigns(CampaignID),
    FOREIGN KEY (InterestID) REFERENCES Interests(InterestID)
);

CREATE TABLE IF NOT EXISTS CampaignsTargetingLocations (
    CampaignTargetLocationID INT PRIMARY KEY AUTO_INCREMENT,
    CampaignID INT NOT NULL,
    LocationID INT NOT NULL,
    FOREIGN KEY (CampaignID) REFERENCES Campaigns(CampaignID),
    FOREIGN KEY (LocationID) REFERENCES Locations(LocationID)
);

CREATE TABLE IF NOT EXISTS Devices (
    DeviceID INT PRIMARY KEY AUTO_INCREMENT,
    Device VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS AdEvents (
    AdEventID CHAR(36) PRIMARY KEY, -- UUID/GUID
    CampaignID INT,
    UserID INT,
    DeviceID INT,
    Timestamp DATETIME,
    BidAmount DECIMAL(10, 2),
    AdCost DECIMAL(10, 2),
    WasClicked BOOLEAN,
    ClickTimestamp DATETIME,
    AdRevenue DECIMAL(10, 2)
);

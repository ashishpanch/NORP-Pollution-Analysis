CREATE TABLE IF NOT EXISTS irs_migration_2015_2016 (
    `county_fips`   VARCHAR(5)  NOT NULL,
    `state`         CHAR(2)      NOT NULL,
    `countyname`    VARCHAR(255) NOT NULL,
    `n1_inflow`     INT          NOT NULL,
    `n1_outflow`    INT          NOT NULL,
    `net_migration` INT          NOT NULL,
    `agi_inflow`    BIGINT       NOT NULL,
    `agi_outflow`   BIGINT       NOT NULL
);

CREATE TABLE IF NOT EXISTS irs_migration_2016_2017 (
    `county_fips`   VARCHAR(5)  NOT NULL,
    `state`         CHAR(2)      NOT NULL,
    `countyname`    VARCHAR(255) NOT NULL,
    `n1_inflow`     INT          NOT NULL,
    `n1_outflow`    INT          NOT NULL,
    `net_migration` INT          NOT NULL,
    `agi_inflow`    BIGINT       NOT NULL,
    `agi_outflow`   BIGINT       NOT NULL
);

CREATE TABLE IF NOT EXISTS irs_migration_2017_2018 (
    `county_fips`   VARCHAR(5)  NOT NULL,
    `state`         CHAR(2)      NOT NULL,
    `countyname`    VARCHAR(255) NOT NULL,
    `n1_inflow`     INT          NOT NULL,
    `n1_outflow`    INT          NOT NULL,
    `net_migration` INT          NOT NULL,
    `agi_inflow`    BIGINT       NOT NULL,
    `agi_outflow`   BIGINT       NOT NULL
);

CREATE TABLE IF NOT EXISTS irs_migration_2018_2019 (
    `county_fips`   VARCHAR(5)  NOT NULL,
    `state`         CHAR(2)      NOT NULL,
    `countyname`    VARCHAR(255) NOT NULL,
    `n1_inflow`     INT          NOT NULL,
    `n1_outflow`    INT          NOT NULL,
    `net_migration` INT          NOT NULL,
    `agi_inflow`    BIGINT       NOT NULL,
    `agi_outflow`   BIGINT       NOT NULL
);

CREATE TABLE IF NOT EXISTS irs_migration_2019_2020 (
    `county_fips`   VARCHAR(5)  NOT NULL,
    `state`         CHAR(2)      NOT NULL,
    `countyname`    VARCHAR(255) NOT NULL,
    `n1_inflow`     INT          NOT NULL,
    `n1_outflow`    INT          NOT NULL,
    `net_migration` INT          NOT NULL,
    `agi_inflow`    BIGINT       NOT NULL,
    `agi_outflow`   BIGINT       NOT NULL
);

CREATE TABLE IF NOT EXISTS irs_migration_2020_2021 (
    `county_fips`   VARCHAR(5)  NOT NULL,
    `state`         CHAR(2)      NOT NULL,
    `countyname`    VARCHAR(255) NOT NULL,
    `n1_inflow`     INT          NOT NULL,
    `n1_outflow`    INT          NOT NULL,
    `net_migration` INT          NOT NULL,
    `agi_inflow`    BIGINT       NOT NULL,
    `agi_outflow`   BIGINT       NOT NULL
);

CREATE TABLE IF NOT EXISTS irs_migration_2021_2022 (
    `county_fips`   VARCHAR(5)  NOT NULL,
    `state`         CHAR(2)      NOT NULL,
    `countyname`    VARCHAR(255) NOT NULL,
    `n1_inflow`     INT          NOT NULL,
    `n1_outflow`    INT          NOT NULL,
    `net_migration` INT          NOT NULL,
    `agi_inflow`    BIGINT       NOT NULL,
    `agi_outflow`   BIGINT       NOT NULL
);
library(fastDummies)
library(DBI)
library(stringr)
library(dplyr)
library(ggplot2)
library(fastDummies)
library(tidyr)
library(rstudioapi)

# Set today's date
# this will be used as a format to pull the scrape tables from postgres
# and to name the output folders
today <- format(Sys.Date(), format = "%Y%m%d")

# use this method if the system date is not the day of the scrape
today <- "20221218"

# Set last week's scrape date
# this will be used to compare with this weeks tables and fill in missing data
lastWeek <- "20221210"

# Set all the years needed based on their postgres database name (the year and the date it was scraped)
getDates <- c("2020_20221112", "2021_20221112", paste("2022_", today, sep = ""))

# use this method if the scrape date is different from today's date
getDates <- c("2020_20221112", "2021_20221112", "2022_20221218")


# the following are base functions to extract the tables from postgres
# based on the input dates above
getCaseOverviews <- function(i){
  db_name <- paste("ojdevictions", i, sep = "_")
  con <- dbConnect(RPostgres::Postgres(),dbname = db_name,
                   host = 'localhost',
                   port = '5432',
                   user = 'postgres',
                   password = 'admin')
  query <- dbSendQuery(con, 'SELECT * FROM "case-overviews"')
  case_overviews <- dbFetch(query)
  dbClearResult(query)
  return(case_overviews)
}

getCaseParties <- function(i){
  db_name <- paste("ojdevictions", i, sep = "_")
  con <- dbConnect(RPostgres::Postgres(),dbname = db_name,
                   host = 'localhost',
                   port = '5432',
                   user = 'postgres',
                   password = 'admin')
  query <- dbSendQuery(con, 'SELECT * FROM "case-parties"')
  case_parties <- dbFetch(query)
  dbClearResult(query)
  return(case_parties)
}

getEvents <- function(i){
  db_name <- paste("ojdevictions", i, sep = "_")
  con <- dbConnect(RPostgres::Postgres(),dbname = db_name,
                   host = 'localhost',
                   port = '5432',
                   user = 'postgres',
                   password = 'admin')
  query <- dbSendQuery(con, 'SELECT * FROM "events"')
  events <- dbFetch(query)
  dbClearResult(query)
  return(events)
}

getFiles <- function(i){
  db_name <- paste("ojdevictions", i, sep = "_")
  con <- dbConnect(RPostgres::Postgres(),dbname = db_name,
                   host = 'localhost',
                   port = '5432',
                   user = 'postgres',
                   password = 'admin')
  query <- dbSendQuery(con, 'SELECT * FROM "files"')
  files <- dbFetch(query)
  dbClearResult(query)
  return(files)
}

getJudgments <- function(i){
  db_name <- paste("ojdevictions", i, sep = "_")
  con <- dbConnect(RPostgres::Postgres(),dbname = db_name,
                   host = 'localhost',
                   port = '5432',
                   user = 'postgres',
                   password = 'admin')
  query <- dbSendQuery(con, 'SELECT * FROM "judgments"')
  judgments <- dbFetch(query)
  dbClearResult(query)
  return(judgments)
}

getLawyers <- function(i){
  db_name <- paste("ojdevictions", i, sep = "_")
  con <- dbConnect(RPostgres::Postgres(),dbname = db_name,
                   host = 'localhost',
                   port = '5432',
                   user = 'postgres',
                   password = 'admin')
  query <- dbSendQuery(con, 'SELECT * FROM "lawyers"')
  lawyers <- dbFetch(query)
  dbClearResult(query)
  return(lawyers)
}

# the base functions are called and the postgres tables are created in R
case_overviews <- Reduce("rbind", lapply(getDates, getCaseOverviews))
case_parties <- Reduce("rbind", lapply(getDates, getCaseParties))
events <- Reduce("rbind", lapply(getDates, getEvents))
judgments <- Reduce("rbind", lapply(getDates, getJudgments))
lawyers <- Reduce("rbind", lapply(getDates, getLawyers))



# Import last weeks scrape tables for comparison
case_overviews_0 <-  readr::read_csv(paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", lastWeek, "/case_overviews.csv", sep = "")) %>% select(-1)
case_parties_0 <- readr::read_csv(paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", lastWeek, "/case_parties.csv", sep = "")) %>% select(-1)
events_0 <- readr::read_csv(paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", lastWeek, "/events.csv", sep = "")) %>% select(-1)
judgments_0 <- readr::read_csv(paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", lastWeek, "/judgments.csv", sep = "")) %>% select(-1)
lawyers_0 <- readr::read_csv(paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", lastWeek, "/lawyers.csv", sep = "")) %>% select(-1)


# Compare last weeks table to the new tables and include cases from last week 
# that are missing in this week's scrape
case_overviews <- rbind(case_overviews, 
                          case_overviews_0 %>% 
                            filter(!(case_code %in% case_overviews$case_code)))

case_parties <- rbind(case_parties,
                      case_parties_0 %>% 
                        filter(!(case_code %in% case_parties$case_code))) 

events <- rbind(events,
                events_0 %>% 
                  filter(!(case_code %in% events$case_code))) 

judgments <- rbind(judgments,
                   judgments_0 %>% 
                     filter(!(case_code %in% judgments$case_code))) 

lawyers <- rbind(lawyers,
                 lawyers_0 %>% 
                   filter(!(case_code %in% lawyers$case_code))) 


# The following are functions to create the flat file summary of important information
# from the base scrape tables

getDefendantInfo <- function() {
  case_parties %>%
    filter(party_side == "Defendant") %>%
    group_by(case_code) %>%
    summarize(defendant_names = paste(name, collapse = "; "),
              defendant_addr = paste(unique(addr), collapse = "; ")) %>% 
    return()
}

getAgent <- function() {
  case_parties %>%
    filter(party_side == "Agent") %>%
    group_by(case_code) %>%
    summarize(Agent = paste(name, collapse = "; ")) %>% 
    return()
}

# This createJudgmentDummies method is no longer in use
# as we have a better method for tabulating judgments
createJudgmentDummies <- function() {
  judgments %>% 
    fastDummies::dummy_cols(select_columns = "case_type") %>% 
    group_by(case_code) %>% 
    summarise_if(is.numeric, sum, na.rm = TRUE) %>% 
    summarise(case_code = case_code,
              Judgment_General = ifelse(`case_type_Judgment - General` > 0 | 
                                          `case_type_Amended Judgment - General` > 0 |
                                          `case_type_Amended Judgment - Corrected General` > 0 | 
                                          `case_type_Judgment - Corrected General` |
                                          `case_type_Judgment - General Creates Lien` > 0 |
                                          `case_type_Amended Judgment - General Creates Lien` > 0 |
                                          `case_type_Judgment - Corrected General Creates Lien` > 0, 1, 0),
              Judgment_Creates_Lien = ifelse(`case_type_Judgment - General Creates Lien` > 0 |
                                               `case_type_Judgment - Supplemental Creates Lien` > 0 |
                                               `case_type_Judgment - Limited Creates Lien` > 0 |
                                               `case_type_Amended Judgment - General Creates Lien` > 0 |
                                               `case_type_Judgment - General Dismissal Creates Lien` > 0 |
                                               `case_type_Amended Judgment - Corrected General Creates Lien` > 0 |
                                               `case_type_Judgment - Corrected General Creates Lien` > 0 |
                                               `case_type_Amended Judgment - Supplemental Creates Lien` > 0 |
                                               `case_type_Amended Judgment - Limited Creates Lien` > 0 |
                                               `case_type_Amended Judgment - Corrected Limited Creates Lien` > 0 |
                                               `case_type_Amended Judgment - Corrected Supplemental Creates Lien` > 0 |
                                               `case_type_Judgment - Corrected Supplemental Creates Lien` > 0 |
                                               `case_type_Judgment - Limited Dismissal Creates Lien` > 0, 1, 0),
              Judgment_Dismissal = ifelse(`case_type_Judgment - General Dismissal` > 0 |
                                            `case_type_Judgment - Limited Dismissal` > 0 |
                                            `case_type_Amended Judgment - General Dismissal` > 0, 1, 0)) %>% 
    #`case_type_Amended Judgment - Limited Dismissal` > 0, 1, 0)) %>% 
    return()
}

addMoratoriumVars <- function() {
 case_overviews %>% 
    mutate(date2 = as.Date(case_overviews$date, "%m/%d/%Y"),
           Oregon_Moratorium = if_else(date2 >= '2020-3-22' & date2 <= '2021-06-30', 1, 0),
           Multnomah_Moratorium = if_else(date2 >= '2020-3-17' & date2 <= '2021-02-01' & location == "Multnomah", 1, 0)) %>% 
    return()
}

getPlaintifNames <- function() {
  case_parties %>% 
    filter(party_side == "Plaintiff") %>% 
    group_by(case_code) %>% 
    summarize(plaintiff_name = paste(name, collapse = "; ")) %>% 
    return()
}

getLawyersByParty <- function() {
  case_parties %>%
    rename(party_name = name) %>% 
    select(case_code, party_name, party_side) %>% 
    right_join(lawyers %>% 
                 rename(lawyerName = name) %>%
                 select(case_code, party_name, lawyerName, status), by = c('case_code', 'party_name')) %>% 
    return()
}

getDefendantLawyers <- function() {
  getLawyersByParty() %>% 
    filter(party_side == "Defendant") %>% 
    group_by(case_code) %>% 
    summarize(party = paste(unique(party_name), collapse = "; "), 
              tenant_lawyer = paste(unique(lawyerName), collapse = "; ")) %>%
    return()
}

getPlaintiffLawyer <- function() {
  getLawyersByParty() %>% 
    filter(party_side == "Plaintiff") %>% 
    group_by(case_code) %>% 
    summarize(party = paste(unique(party_name), collapse = "; "), 
              landlord_lawyer = paste(unique(lawyerName), collapse = "; ")) %>% 
    return()
}

# The following 3 functions: 
# makeFTAVars, makeFTADefault, and makeFTAFirstAppearance are older 
# and should be revised
makeFTAvars <- function() {
  #makes Failure to Appear variable
  events %>% 
    filter(result == "FTA - Default" | result == "Failure to Appear") %>% 
    distinct(case_code) %>% 
    mutate(FTA = 1) %>% 
    return()
}

makeFTADefault <- function() {
  #makes Failure to Appear variable
  events %>% 
    filter(result == "FTA - Default") %>% 
    distinct(case_code) %>% 
    mutate(FTADefault = 1) %>% 
    return()
}

makeFTAFirstAppearance <- function() {
  events %>% 
    filter(grepl("hearing", title, ignore.case = TRUE)) %>%
    mutate(firstHearing = !duplicated(case_code)) %>% 
    select(firstHearing, case_code, title, result) %>% 
    filter(firstHearing == "TRUE") %>% 
    filter(grepl("FTA|Failure to Appear", result, ignore.case = TRUE)) %>% 
    mutate(FTAFirst = 1) %>% 
    return()
}

# makeFlatFile compiles all the synthesized functions above and creates
# the flatfile summary data frame with key selected variables
makeFlatFile <- function() {
  # makes final flat_file output
  addMoratoriumVars() %>% 
    select(case_code, style, date, Oregon_Moratorium, Multnomah_Moratorium, status, location) %>% 
    full_join(getPlaintifNames() %>% select(case_code, plaintiff_name), by = 'case_code') %>% 
    full_join(getDefendantInfo() %>% select(case_code, defendant_names, defendant_addr), by = 'case_code') %>% 
    full_join(getAgent() %>% select(case_code, Agent)) %>% 
    # full_join(createJudgmentDummies() %>% select(case_code, Judgment_General, 
    #                                              Judgment_Creates_Lien, 
    #                                              Judgment_Dismissal), by = 'case_code') %>% 
    full_join(getDefendantLawyers() %>% select(case_code, tenant_lawyer), by = 'case_code') %>% 
    full_join(getPlaintiffLawyer() %>% select(case_code, landlord_lawyer), by = 'case_code') %>%
    mutate(landlord_has_lawyer = ifelse(is.na(landlord_lawyer), 0, 1),
           tenant_has_lawyer = ifelse(is.na(tenant_lawyer), 0, 1),
           # FTA = ifelse(is.na(FTA), 0, 1),
           # FTADefault = ifelse(is.na(FTADefault), 0, 1),
           # FTAFirst = ifelse(is.na(FTAFirst), 0, 1),
           # FTAFirstXJudgmentGeneral = ifelse(Judgment_General == 1 & FTAFirst == 1, 1, 0),
           date = as.Date(date, "%m/%d/%Y"),
           month = as.Date(cut(date, breaks = "month")),
           # no_judgment = ifelse(status == "Closed" & judgment == "NULL", 1, 0),
           zip = word(defendant_addr, -1)) %>% 
    rename(case_name = style) %>% 
    return()
}



# this is the flat_file object created from the above method
flat_file <- makeFlatFile()



# The following is based on a project to re-code variables summarized 
# from the scrape tables

# JudgmentsSort are all judgments minus supplemental, vacated, and set-aside judgments
# this is a method to distill the final judgment from cases with multiple judgments
# the JudgmentsSort data frame is created by filtering all judgments by the latest judgment date

# n = the number of total judgments
# nDistinct = the number of distinct judgment types per case
# judgment = all distinct judgments per case
# maxDate = is the most recent date a judgment was rendered

judgmentsSort <- judgments %>% 
  mutate(date = as.Date(date, "%m/%d/%Y")) %>% 
  filter(!grepl("supplemental", case_type, ignore.case = T)) %>% 
  filter(!grepl("vacated", decision, ignore.case = T)) %>% 
  filter(!grepl("set aside|set-aside", decision, ignore.case = T)) %>% 
  group_by(case_code) %>% 
  filter(date == max(date)) %>%
  summarise(n=n(),
            nDistinct = n_distinct(case_type),
            judgment = paste(unique(case_type), collapse = "; "),
            maxDate = max(date)) 


# LienJudgments are a data frame of judgments that contain "lien" in the name
# and are not vacated
LienJudgments <- judgments %>% 
  filter(grepl("lien", case_type, ignore.case = T)) %>% 
  filter(!grepl("vacated", decision, ignore.case = T))

# faEvents is a list of events that signify first appearance 
faEvents <- c("Hearing - Landlord/Tenant",
              "Hearing - Initial Appearance",
              "Hearing",
              "Appearance")

# faCases are events with titles matching the above list of first appearance events
faCases <- events %>% 
  mutate(date = as.Date(date, "%m/%d/%Y")) %>% 
  filter(date < "2022-09-09") %>% 
  filter(title %in% faEvents)


stipulated_events <- c("Affidavit/Declaration - Non-Compliance",
                       "Agreement",
                       "Agreement - Mediation",
                       "Hearing - Mediation",
                       "Hearing - Non Compliance",
                       "Notice - Non-Compliance",
                       "Order - Stipulated")

saJudgments <- judgments %>% filter(grepl("stip", decision, ignore.case = T))

saCases <- events %>% filter(title %in% stipulated_events)

writCases <- events %>% filter(grepl("writ", title, ignore.case = T))

generalJudgments <- c("Amended Judgment - General",
                      "Amended Judgment - General Creates Lien",
                      "Judgment - Corrected General",
                      "Judgment - Corrected General Creates Lien",
                      "Judgment - General",
                      "Judgment - General Creates Lien")

tdefEvents <- events %>%
  filter(grepl("Failure to Appear|FTA", result, ignore.case = T))

tdefJudgments <- judgments %>% 
  filter(grepl("FTA|default", decision, ignore.case = T))


flat_fileV2 <- flat_file %>% 
  left_join(judgmentsSort %>% select(case_code, judgment), by = "case_code") %>% 
  mutate(judgment = ifelse(is.na(judgment), "No Judgment", ifelse(grepl(";", judgment), "MULTIPLE", judgment)))

flat_fileV2 <- flat_fileV2 %>% 
  mutate(Judgment_General = ifelse(judgment %in% generalJudgments, 1, 0),
         Judgment_Dismissal = ifelse(grepl("Dismissal", judgment), 1, 0),
         Judgment_Creates_Lien = ifelse(case_code %in% LienJudgments$case_code, 1, 0),
         FA = ifelse(case_code %in% faCases$case_code, 1, 0),
         SA = ifelse(case_code %in% c(saCases$case_code, saJudgments$case_code), 1, 0),
         writ = ifelse(case_code %in% writCases$case_code, 1, 0),
         Default = ifelse(case_code %in% c(tdefEvents$case_code, tdefJudgments$case_code), 1, 0)
  )

flat_fileV2 <- flat_fileV2 %>% 
  # Make Status Variables
  mutate(OPEN = ifelse(status == "Open", 1, 0),
         PENDING = ifelse(grepl("Appeal|Arbitration|Reinstated|Stayed|Closed", status) & Judgment_General == 0 & Judgment_Dismissal == 0 & Judgment_Creates_Lien == 0, 1, 0),
         CLOSED = ifelse(status == "Closed" & PENDING == 0, 1, 0)) %>% 
  
  # binary agent variables
  mutate(AGENT = ifelse(is.na(Agent) == FALSE, 1, 0)) %>% 
  rename(Agent_Name = Agent) %>% 
  
  # separate defendant names
  cbind(stringr::str_split_fixed(flat_file$defendant_names, ", |; ", 3)) %>% 
  rename(head_last_name = "1",
         head_first_name = "2",
         other_party = "3"
  )



dir.create(paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, sep =""))

saveTablesCSV <- function(){
  write.csv(flat_fileV2, paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, "/flat_file.csv", sep = ""))
  write.csv(case_overviews, paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, "/case_overviews.csv", sep = ""))
  write.csv(case_parties, paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, "/case_parties.csv", sep = ""))
  write.csv(events, paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, "/events.csv", sep = ""))
  write.csv(judgments, paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, "/judgments.csv", sep = ""))
  write.csv(lawyers, paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, "/lawyers.csv", sep = ""))
  # write.csv(files, paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, "/files.csv", sep = ""))
}


saveTablesCSV()


# update county summaries

county_summs2020 <- read.csv("county_summs2020.csv")
county_summs2020 <- county_summs2020 %>% select(-1)

flat_fileV2 %>%
  filter(date >= "2022-01-01") %>% 
  group_by(location) %>% 
  summarize(Filings = n(), 
            Dismissals = sum(na.omit(Judgment_Dismissal) == 1), 
            `Judgments to Evict` = sum(na.omit(Judgment_General) == 1),
            `Open Cases` = sum(OPEN == 1),
            `Landlord has Lawyer` = sum(na.omit(landlord_has_lawyer) == 1),
            `Tenant has Lawyer` = sum(na.omit(tenant_has_lawyer) == 1)) %>% 
  add_row(location = "Oregon",
          Filings = sum(.$Filings), 
          Dismissals = sum(.$Dismissals),
          `Judgments to Evict` = sum(.$`Judgments to Evict`),
          `Open Cases` = sum(.$`Open Cases`),
          `Landlord has Lawyer` = sum(.$`Landlord has Lawyer`),
          `Tenant has Lawyer` = sum(.$`Tenant has Lawyer`)) %>% 
  left_join(county_summs2020, by = c("location"="NAM")) %>% 
  select(-GEOID) %>% 
  rename(`Poverty Rate` = AllPov,
         `Percent POC` = POC,
         `Number of Renter Units` = RentrUnts,
         `Percent Renter Units` = RentrUntsP) %>% 
  mutate(`Filing Rate per 100 Rental Units` = Filings*100 / `Number of Renter Units`) %>% 
  select(-NHWhite) %>%  
  write.csv(paste("G:/Shared drives/ojdevictions/ScrapeData/CountySummaries/CountySummary", today, ".csv", sep = ""))


# Weekly Judgments Multnomah County

judgments %>% 
  mutate(date = as.Date(date, "%m/%d/%Y")) %>% 
  left_join(case_overviews %>% select(case_code, location)) %>% 
  data.frame() %>% 
  filter(date >= "2022-01-01") %>%
  filter(location == "Multnomah") %>% 
  group_by(location, case_type, week = lubridate::floor_date(date, "week")) %>% 
  summarise(n=n()) %>% 
  tidyr::spread(case_type, n) %>% 
  write.csv(paste("G:/Shared drives/ojdevictions/ScrapeData/Weekly_Judgments/WeeklyJudgmentsMultnomahCo_2022_", today, ".csv", sep = ""))









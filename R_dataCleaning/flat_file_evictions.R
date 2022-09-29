library(fastDummies)
library(DBI)
library(stringr)
library(dplyr)
library(ggplot2)
library(fastDummies)
library(tidyr)
library(rstudioapi)

#Set today
today <- Sys.Date()
today <- format(today, format = "%Y%m%d")

connectDB <- function() {
  db_name20 <- "20210913_ojdevictions_2020"
  db_name21 <- paste("ojdevictions_2021_", today, sep = "")
  # db_name20 <- paste(today, "_ojdevictions_2020", sep = "")
  # db_name21 <- paste(today, "_ojdevictions_2021", sep = "")
  
  con20 <- dbConnect(RPostgres::Postgres(),dbname = db_name20,
                     host = 'localhost',
                     port = '5432',
                     user = 'postgres',
                     password = 'admin')
  
  con21 <- dbConnect(RPostgres::Postgres(),dbname = db_name21,
                     host = 'localhost',
                     port = '5432',
                     user = 'postgres',
                     password = 'admin')
  return(c(con20, con21))
}

getCaseOverviews <- function() {
  con20 <- connectDB()[[1]]
  con21 <- connectDB()[[2]]
  
  query20 <- dbSendQuery(con20, 'SELECT * FROM "case-overviews"')
  case_overviews20 <- dbFetch(query20)
  dbClearResult(query20)
  
  query21 <- dbSendQuery(con21, 'SELECT * FROM "case-overviews"')
  case_overviews21 <- dbFetch(query21)
  dbClearResult(query21)
  
  case_overviews <- bind_rows(case_overviews20, case_overviews21)
  case_overviews$style <- str_replace_all(case_overviews$style, "\n", " ")
  return(case_overviews)
}

getCaseParties <- function() {
  con20 <- connectDB()[[1]]
  con21 <- connectDB()[[2]]
  
  query20 <- dbSendQuery(con20, 'SELECT * FROM "case-parties"')
  case_parties20 <- dbFetch(query20)
  dbClearResult(query20)
  
  query21 <- dbSendQuery(con21, 'SELECT * FROM "case-parties"')
  case_parties21 <- dbFetch(query21)
  dbClearResult(query21)
  
  case_parties <- bind_rows(case_parties20, case_parties21)
  
  return(case_parties)
}
getEvents <- function() {
  con20 <- connectDB()[[1]]
  con21 <- connectDB()[[2]]
  
  query20 <- dbSendQuery(con20, 'SELECT * FROM "events"')
  events20 <- dbFetch(query20)
  dbClearResult(query20)
  
  query21 <- dbSendQuery(con21, 'SELECT * FROM "events"')
  events21 <- dbFetch(query21)
  dbClearResult(query21)
  
  events <- bind_rows(events20, events21)
  return(events)
}
getFiles <- function() {
  con20 <- connectDB()[[1]]
  con21 <- connectDB()[[2]]
  
  query20 <- dbSendQuery(con20, 'SELECT * FROM "files"')
  files20 <- dbFetch(query20)
  dbClearResult(query20)
  
  query21 <- dbSendQuery(con21, 'SELECT * FROM "files"')
  files21 <- dbFetch(query21)
  dbClearResult(query21)
  
  files <- bind_rows(files20, files21)
  return(files)
}

getJudgments <- function() {
  con20 <- connectDB()[[1]]
  con21 <- connectDB()[[2]]
  
  query20 <- dbSendQuery(con20, 'SELECT * FROM "judgments"')
  judgments20 <- dbFetch(query20)
  dbClearResult(query20)
  
  query21 <- dbSendQuery(con21, 'SELECT * FROM "judgments"')
  judgments21 <- dbFetch(query21)
  dbClearResult(query21)
  
  judgments <- bind_rows(judgments20, judgments21)
  return(judgments)
}
getLawyers <- function() {
  con20 <- connectDB()[[1]]
  con21 <- connectDB()[[2]]
  
  query20 <- dbSendQuery(con20, 'SELECT * FROM "lawyers"')
  lawyers20 <- dbFetch(query20)
  dbClearResult(query20)
  
  query21 <- dbSendQuery(con21, 'SELECT * FROM "lawyers"')
  lawyers21 <- dbFetch(query21)
  dbClearResult(query21)
  
  lawyers <- bind_rows(lawyers20, lawyers21)
  return(lawyers)
}

getDefendantInfo <- function() {
  getCaseParties() %>%
    filter(party_side == "Defendant") %>%
    group_by(case_code) %>%
    summarize(defendant_names = paste(name, collapse = "; "),
              defendant_addr = paste(unique(addr), collapse = "; ")) %>% 
    return()
}

getAgent <- function() {
  getCaseParties() %>%
    filter(party_side == "Agent") %>%
    group_by(case_code) %>%
    summarize(Agent = paste(name, collapse = "; ")) %>%
    return()
}

# setwd(dirname(getActiveDocumentContext()$path))

createJudgmentDummies <- function() {
  getJudgments() %>% 
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
  getCaseOverviews() %>% 
    mutate(date2 = as.Date(getCaseOverviews()$date, "%m/%d/%Y"),
           Oregon_Moratorium = if_else(date2 >= as.Date('2020-3-22'), 1, 0),
           Multnomah_Moratorium = if_else(date2 >= as.Date('2020-3-17') & location == "Multnomah", 1, 0)) %>% 
    return()
}

getPlaintifNames <- function() {
  getCaseParties() %>% 
    filter(party_side == "Plaintiff") %>% 
    group_by(case_code) %>% 
    summarize(plaintiff_name = paste(name, collapse = "; ")) %>% 
    return()
}

getLawyersByParty <- function() {
  getCaseParties() %>%
    rename(party_name = name) %>% 
    select(case_code, party_name, party_side) %>% 
    right_join(getLawyers() %>% 
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

makeFTAvars <- function() {
  #makes Failure to Appear variable
  getEvents() %>% 
    filter(result == "FTA - Default" | result == "Failure to Appear") %>% 
    distinct(case_code) %>% 
    mutate(FTA = 1) %>% 
    return()
}

makeFTADefault <- function() {
  #makes Failure to Appear variable
  getEvents() %>% 
    filter(result == "FTA - Default") %>% 
    distinct(case_code) %>% 
    mutate(FTADefault = 1) %>% 
    return()
}

#x <- makeFTADefault()

makeFTAFirstAppearance <- function() {
  getEvents() %>% 
    filter(grepl("hearing", title, ignore.case = TRUE)) %>%
    mutate(firstHearing = !duplicated(case_code)) %>% 
    select(firstHearing, case_code, title, result) %>% 
    filter(firstHearing == "TRUE") %>% 
    filter(grepl("FTA|Failure to Appear", result, ignore.case = TRUE)) %>% 
    mutate(FTAFirst = 1) %>% 
    return()
}

makeFlatFile <- function() {
  # makes final flat_file output
  addMoratoriumVars() %>% 
    select(case_code, style, date, Oregon_Moratorium, Multnomah_Moratorium, status, location) %>% 
    full_join(getPlaintifNames() %>% select(case_code, plaintiff_name), by = 'case_code') %>% 
    full_join(getDefendantInfo() %>% select(case_code, defendant_names, defendant_addr), by = 'case_code') %>%
    full_join(getAgent() %>% select(case_code, Agent)) %>%
    full_join(createJudgmentDummies() %>% select(case_code, Judgment_General, 
                                       Judgment_Creates_Lien, 
                                       Judgment_Dismissal), by = 'case_code') %>% 
    full_join(getDefendantLawyers() %>% select(case_code, tenant_lawyer), by = 'case_code') %>% 
    full_join(getPlaintiffLawyer() %>% select(case_code, landlord_lawyer), by = 'case_code') %>% 
    full_join(makeFTAvars(), by = 'case_code') %>%
    full_join(makeFTADefault(), by = 'case_code') %>%
    full_join(makeFTAFirstAppearance() %>% select(case_code, FTAFirst), by = 'case_code') %>% 
    mutate(landlord_has_lawyer = ifelse(is.na(landlord_lawyer), 0, 1),
           tenant_has_lawyer = ifelse(is.na(tenant_lawyer), 0, 1),
           FTA = ifelse(is.na(FTA), 0, 1),
           FTADefault = ifelse(is.na(FTADefault), 0, 1),
           FTAFirst = ifelse(is.na(FTAFirst), 0, 1),
           FTAFirstXJudgmentGeneral = ifelse(Judgment_General == 1 & FTAFirst == 1, 1, 0),
           date = as.Date(date, "%m/%d/%Y"),
           month = as.Date(cut(date, breaks = "month")),
           # no_judgment = ifelse(status == "Closed" & judgment == "NULL", 1, 0),
           zip = word(defendant_addr, -1)) %>% 
    rename(case_name = style) %>% 
    return()
}

saveTablesRDS <- function() {
  saveRDS(makeFlatFile(), "flat_file.rds")
  saveRDS(getCaseOverviews(),  "case_overviews.rds")
  saveRDS(getCaseParties(), "case_parties.rds")
  saveRDS(getEvents(), "events.rds")
  saveRDS(getJudgments(), "judgments.rds")
  saveRDS(getLawyers(), "lawyers.rds")
  saveRDS(getFiles(),  "files.rds")
  }

saveTablesCSV <- function(){
  write.csv(makeFlatFile(), paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, "/flat_file.csv", sep = ""))
  write.csv(getCaseOverviews(), paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, "/case_overviews.csv", sep = ""))
  write.csv(getCaseParties(), paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, "/case_parties.csv", sep = ""))
  write.csv(getEvents(), paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, "/events.csv", sep = ""))
  write.csv(getJudgments(), paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, "/judgments.csv", sep = ""))
  write.csv(getLawyers(), paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, "/lawyers.csv", sep = ""))
  write.csv(getFiles(), paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, "/files.csv", sep = ""))
}

# set save directory
#today <- "20210921"

# dir.create(paste("output/fullScrape", today, sep = ""))
# dir.create(paste("output/csv/full_Scrape", today, sep = "_"))

# Execute
# saveTablesRDS()

dir.create(paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape_", today, sep =""))
saveTablesCSV()

# file.copy(paste("output/csv/full_scrape", today, sep = "_"), paste("G:/Shared drives/ojdevictions/ScrapeData/full_scrape", today, sep ="_"), recursive = TRUE)






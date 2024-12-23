
############### 01 - Setup ###############
start.time <- Sys.time()


# Define Path of File to be Loaded
file_path = "~/Downloads/"


# Load Packages
library(tidyverse)
library(arrow)
library(data.table)
select = dplyr::select


# Load Data
player_sims = read_parquet(paste0(file_path, "ETR_SimOutput_2024_15.parquet"))


# Conver to a data.table
setDT(player_sims)


# Assume No Games have started player this week
tms_not_started = unique(player_sims$Team)


############### 02 - Re-Format Data ###############


## Define Percentiles to Save
percentile_seq = seq(0.01, 0.99, 0.01)


## Aggregate Data to the nearest percentile
percentiles = player_sims[Team %in% tms_not_started, 
                          .(percentile = percentile_seq,
                            pass_att = quantile(sim_pass_attempts, percentile_seq),
                            pass_comp = quantile(sim_comp, percentile_seq),
                            pass_td = quantile(sim_tds_pass, percentile_seq),
                            pass_int = quantile(sim_ints, percentile_seq),
                            pass_yards = quantile(sim_pass_yards, percentile_seq),
                            pass_longest = quantile(sim_longest_pass, percentile_seq),
                            rush_att = quantile(sim_rush_attempts, percentile_seq),
                            rush_yards = quantile(sim_rush_yards, percentile_seq),
                            rush_longest = quantile(sim_longest_rush, percentile_seq),
                            targets = quantile(sim_targets, percentile_seq),
                            catches = quantile(sim_receptions, percentile_seq),
                            rec_yards = quantile(sim_rec_yards, percentile_seq),
                            rec_longest = quantile(sim_longest_reception, percentile_seq),
                            pass_rush_td = quantile(sim_tds_rush_pass, percentile_seq),
                            rec_rush_td = quantile(sim_tds_rush_rec, percentile_seq),
                            pass_rush_yards = quantile(sim_yards_rush_pass, percentile_seq),
                            rec_rush_yards = quantile(sim_yards_rush_rec, percentile_seq)
                          ), 
                          by = .(Season, Week, GameSimID, Team, Opponent, Player, Position, GSISID, PlayerID)]


# Replace NAs
percentiles[is.na(percentiles)] = 0


# Round Values
percentiles = percentiles[, lapply(.SD, function(x) if (is.numeric(x)) round(x, 2) else x)]


# Sort table
setorder(percentiles, Team, Position, percentile)


# Identify Players with to be included
unique_players = unique(percentiles[
  percentile == 0.20 & (pass_yards > 0 | rush_yards > 0 | rec_yards > 0) & Position %in% c("QB", "RB", "WR", "TE"), 
  PlayerID
])


# Aggregate QB Data
alt_calc_data_qb = percentiles[PlayerID %in% unique_players & Position=='QB',]
alt_calc_data_qb = melt(
  alt_calc_data_qb, 
  id.vars = c("Player","percentile"), 
  measure.vars = c("rush_yards", "rush_att", "rush_longest", "pass_att", "pass_comp","pass_yards","pass_int","pass_longest","pass_td","pass_rush_td","pass_rush_yards"), 
  variable.name = "stat", 
  value.name = "value")


# Aggregate Pass Catcher (PC) Data
alt_calc_data_pc = percentiles[PlayerID %in% unique_players & Position %in% c('WR','TE'),]
alt_calc_data_pc = melt(
  alt_calc_data_pc, 
  id.vars = c("Player","percentile"), 
  measure.vars = c("rec_yards","catches","targets","rec_longest","rec_rush_td"), 
  variable.name = "stat", 
  value.name = "value")

# Aggregate RB Data
alt_calc_data_rb = percentiles[PlayerID %in% unique_players & Position %in% c('RB'),]
alt_calc_data_rb = melt(
  alt_calc_data_rb, 
  id.vars = c("Player","percentile"), 
  measure.vars = c("rush_yards", "rush_att", "rush_longest", "rec_yards", "catches", "targets", "rec_longest", "rec_rush_td", "rec_rush_yards"), 
  variable.name = "stat", 
  value.name = "value")


# Combine All Data
alt_calc_data = rbindlist(list(alt_calc_data_qb, alt_calc_data_rb, alt_calc_data_pc), use.names = TRUE, fill = TRUE)


# Sort Data
setorder(alt_calc_data, Player, stat, percentile)


############### 03 - Save Data ###############

# Load Necessary Packages
library(googlesheets4)
library(googledrive)


# Define ID of google sheet where data should be pushed
sheet_url = "https://docs.google.com/spreadsheets/d/1W4PbX4QX9_nmQqzp6poIAgAb7wv1v7-1MKo0Ve_86gY/edit?gid=0#gid=0"


# Authenticate Google Account
## NOTE: 
# THE FIRST TIME YOU RUN THE CODE BELOW (OR ANYTHING FROM THE GOOGLESHEETS4 OR GOOGLEDRIVE PACKAGES) IT WILL ASK YOU TO CONFIRM ACCESS TO YOUR GOOGLE ACCOUNT. 
# AFTER YOU ALLOW IT TO CONNECT TO A GOOGLE ACCOUNT, IT SHOULD ALLOW YOU TO ACCESS THE SHEET WITHOUT A PROBLEM
# THE SHEET IS PUBLIC, SO ANYONE / ANY GOOGLE ACCOUNT SHOULD BE ABLE TO ACCESS IT
# can specify like this to do it manually via broswer: gs4_auth()
# or like this to pass an email: gs4_auth(email = "samw@establishtherun.com")
gs4_auth()


# Clear Existing Data
range_clear(ss = sheet_url, sheet = "raw_data", reformat = F)


# Write new data
range_write(ss = sheet_url, sheet = "raw_data", reformat = F, data = alt_calc_data)


## Save Data Locally 
write_parquet(alt_calc_data, paste0(file_path, "ETR_SimPercentiles_2024_15.parquet"))

end.time <- Sys.time()
time.taken <- round(end.time - start.time,2)
time.taken


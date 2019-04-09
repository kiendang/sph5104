library(RPostgreSQL)
library(tidyverse)

source("utils.R")

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv,
                 dbname = "mimic",
                 host = "172.16.201.249",
                 user = "sph5104",
                 password = trimws(readLines("PASSWORD")))
dbSendQuery(con, "set search_path=group1,public,mimiciii;")

sqls <- c("population", "cohort")

sqls %>% walk(function(sql) {
    sql %>%
    file_to_sql_view(fname = sprintf("%s.sql", sql), title = .) %>%
    dbSendQuery(con, .)
})

dbDisconnect(con)
dbUnloadDriver(drv)

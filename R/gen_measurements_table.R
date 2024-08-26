#' Get the index data
#'
#'
#'
#' @export
gen_measurements_table <- function(dbf,db=F,ow=F){
  if(db){
    dbf <- dbif
  }

  measurements_sql <- str_c("
  SELECT DISTINCT
    p.patid AS 'person.id',
    m.mtype AS 'measurement.id',
    o.obsdate AS 'measurement.date',
    o.value AS 'value.as.number',
    m.value AS 'value.as.category'
  FROM 
    patients AS p
  INNER JOIN
    observations AS o
    ON p.patid=o.patid
  INNER JOIN
    measurement_codes AS m
    ON m.medcodeid=o.medcodeid
  WHERE 
    (o.value IS NOT NULL OR m.value IS NOT NULL) AND
    p.regstartdate <= o.obsdate AND
    (p.regenddate IS NULL OR o.obsdate <= p.regenddate)")

  dbc <- dbConnect(duckdb(),dbf)
  res <- dbGetQuery(dbc,measurements_sql) %>% tibble() %>%
    dplyr::mutate(value.as.number=ifelse(!is.na(value.as.category),NA,value.as.number))
  dbDisconnect(dbc)
  return(res) 
}

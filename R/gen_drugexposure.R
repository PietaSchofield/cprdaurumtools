#' generate a drug exposures table
#'
#'
#' @export
gen_drugexposure <- function(dbf,newtab,tabname,substancefield){
  if(F){
    tabname <- "exposure_codes"
    newtab <- "drug_exposures"
    substancefield <- "substancelink"
    dbf <- dbif
  }

  dec_sql <- str_c("
    CREATE TABLE 
      ",newtab," AS
    SELECT DISTINCT
      d.*,
      e.",substancefield," AS substance
    FROM 
      drug_issues AS d
    INNER JOIN
      ",tabname," AS e
      ON e.prodcodeid=d.prodcodeid")

  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  if(!newtab %in% dbListTables(dbi)){
    val <- dbExecute(dbi,dec_sql)
    ext <- "new records"
  }else{
    val <- dbGetQuery(dbi, paste0("SELECT COUNT(*) FROM ",newtab))
    ext <- "records exist"
  }
  dbDisconnect(dbi)
  return(paste(val,ext))
}


#' extract outcome events
#'
#' 
#'
#'
#' @export
extract_outcomes <- function(dbf,otab,octab,mc1,mc2,mc3,ot="o",bt="b",db=F,ow=F,etab,dbo){
  if(db){
    dbf <- dbif
    dbo <- dbof
    otab <- "hes_diagnosis_epi"
    otab <- "hes_diagnosis_hosp"
    octab <- "outcome_codes"
    etab <- "hes_outcomes"
    ot <- "o"
    bt <- "b"
    mc1 <- "REPLACE(o.icd,'.','')"
    mc2 <- "LIKE"
    mc3 <-  "b.lucode"
  }

  outcome_sql <- str_c("
    SELECT
      ",ot,".* ,
      ",mc1," AS matchcode ,
      ",mc3," AS baitcode
    FROM
      ",otab," AS ",ot," 
    INNER JOIN
      ",octab," AS ",bt,"
      ON ",paste(mc1,mc2,mc3)," ;")

  if(db) writeLines(outcome_sql)

  
  dboc <- duckdb::dbConnect(duckdb::duckdb(),dbo)
  if(!etab%in%dbListTables(dboc) | ow){
    dbic <- duckdb::dbConnect(duckdb::duckdb(),dbf)
    res <- dbGetQuery(dbic,outcome_sql) %>% tibble()
    duckdb::dbDisconnect(dbic,shutdown=F)
    dbWriteTable(dboc,etab,res,over=T)
    rval <- nrow(res)
  }else{
    ret_sql <- str_c("SELECT COUNT(*) FROM ",etab,";")
    rval <- dbGetQuery(dboc,ret_sql)
  }
  duckdb::dbDisconnect(dboc,shutdown=F)
  rval
}

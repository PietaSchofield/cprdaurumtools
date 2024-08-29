#' generate diagnosis index dates
#'
#' Possible options need to
#'   check post yob, 
#'   check within registration
#'   check within study
#'   check prior study start
#'   check priod study end
#'   check prior registration end
#'   check post registration start
#'
#' @export
gen_diagnosis_index <- function(dbf,db=F,diag_tab,idate_tab,sp_start,sp_end,
                                chk_yob=T,chk_rsd=T,chk_sps=T,chk_spe=T,chk_red=T,
                                silent=T,clean=T){
  if(db){
    dbf <- sadb
    diag_tab <- "condition_codes"
    idate_tab <- "baseline_date"
    chk_yob <- T
    chk_rsd <- T
    chk_red <- T
    chk_spe <- T
    chk_sps <- T
    sp_start <- '2005-01-01'
    sp_end <- '2024-12-31'
  }

  idx_sql <- str_c("
    SELECT DISTINCT
      p.patid,
      p.yob,
      p.regstartdate,
      p.regenddate,
      CAST('",sp_start,"' AS DATE) AS studystartdate,
      CAST('",sp_end,"' AS DATE) AS studyenddate,
      indexdate,
      indexcode,
      p.regstartdate < indexdate AS rsd,
      CASE WHEN p.regenddate IS NOT NULL THEN p.regenddate > indexdate ELSE TRUE END AS red,
      CAST('",sp_start,"' AS DATE) < CAST(indexdate AS DATE) AS sps,
      CAST('",sp_end,"' AS DATE) > CAST(indexdate AS DATE) AS spe
    FROM 
      patients AS p
    LEFT JOIN(
      SELECT DISTINCT
        o.patid,
        FIRST(o.obsdate) OVER (PARTITION BY o.patid ORDER BY o.obsdate) AS indexdate,
        FIRST(o.medcodeid) OVER (PARTITION BY o.patid ORDER BY o.obsdate) AS indexcode
      FROM
        patients AS r
      INNER JOIN
        observations AS o
        ON r.patid=o.patid
      INNER JOIN 
        ",diag_tab," AS c
        ON c.medcodeid = o.medcodeid) AS t
      ON t.patid=p.patid")


  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  idx_vals <- dbGetQuery(dbi,idx_sql) %>% tibble()
  dbDisconnect(dbi)

  cleanidates <- idx_vals %>% 
    mutate(basedate=ifelse(rsd & red & sps & spe, indexdate,
                  ifelse(red & sps & spe, regstartdate,
                   ifelse(sps & spe , regenddate, 
                    ifelse(spe, sp_start,sp_end))))) %>%
   select(patid,yob,indexdate,basedate)

  if(silent){
    dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
    dbWriteTable(dbi,idate_tab,cleanidates,over=T)
    dbDisconnect(dbi)
    return(nrow(cleanidates))
  }else{
    if(clean){
      return(cleanidates)
    }else{
      return(idx_vals)
    }
  }
}

#' get index date for cases
#'
#'
#' @import magrittr
#' @export
get_index_cases <- function(ddb){
  ssql <- str_c("
  SELECT DISTINCT
    CASE WHEN p.ibd>0 THEN 'cases' ELSE 'conts' END AS cohort, p.patid,o.medcodeid,
    FIRST(o.obsdate) AS indexdate 
  FROM (
    SELECT
      emis.medcodeid AS medcodeid,
    FROM 
      emismedicaldictionary AS emis
    WHERE 
      CAST(emiscodecategoryid AS INTEGER) IN (15,27,32)) as c
  INNER JOIN observations AS o
  ON o.medcodeid = c.medcodeid
  INNER JOIN patient_cohorts AS p
  ON p.patid = o.patid
  GROUP BY CASE WHEN p.ibd>0 THEN 'cases' ELSE 'conts' END,p.patid,o.medcodeid")

}

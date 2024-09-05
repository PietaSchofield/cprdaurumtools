#' Make a control cohort using some inclusion and exclusion lists
#'
#' @export
make_control_cohort <- function(dbn,exclude,include,db=T,silent=F){
  if(db){
    exclude <- case %>% select(patid)
    include <- poss %>% select(patid)
  }
  cprdaurumtools::load_table(dbf=dbn,dataset=exclude,tab_name="exclude",ow=T)
  cprdaurumtools::load_table(dbf=dbn,dataset=include,tab_name="include",ow=T)

  sample_plan_sql <- str_c("
    SELECT DISTINCT
      prac.region,
      pat.yob,
      pat.gender,
      COUNT(*) AS nums
    FROM
      acceptable_patients AS pat
    INNER JOIN 
      exclude AS ic
      ON ic.patid=pat.patid
    INNER JOIN
      practices AS prac
      ON prac.pracid=pat.pracid
   INNER JOIN
     linkages AS lnk
     ON lnk.patid=pat.patid
   WHERE
     lnk.hes_apc_e LIKE '1' AND
     lnk.ons_death_e LIKE '1' AND
     lnk.lsoa_e LIKE '1' AND
     lnk.hes_op_e LIKE '1' AND
     lnk.hes_ae_e LIKE '1'
    GROUP BY
      pat.yob,
      pat.gender,
      prac.region")
  
  sample_plan <- get_table(dbf=dbn,sqlstr=sample_plan_sql)

  get_possibles_sql <- str_c("
   SELECT DISTINCT
     pat.patid,
     pra.region,
     pat.yob,
     pat.gender
   FROM 
     acceptable_patients AS pat
   INNER JOIN
     practices AS pra
     ON pra.pracid=pat.pracid
   LEFT JOIN
     exclude AS exc
     ON exc.patid=pat.patid
   INNER JOIN
     include AS inc
     ON inc.patid=pat.patid
   INNER JOIN
     linkages AS lnk
     ON lnk.patid=pat.patid
   WHERE
     exc.patid IS NULL AND
     lnk.hes_apc_e LIKE '1' AND
     lnk.ons_death_e LIKE '1' AND
     lnk.lsoa_e LIKE '1' AND
     lnk.hes_op_e LIKE '1' AND
     lnk.hes_ae_e LIKE '1'")

  possibles <- get_table(dbf=dbn,sqlstr=get_possibles_sql)

  sampled_data <- sample_plan %>%
    left_join(possibles, by = c("region","yob","gender")) %>%
    group_split(region,yob, gender) 

  sampled_data1 <- lapply(sampled_data,function(x){
    ss <- x %>% pull(nums) %>% unique()
    slice_sample(x, n=5*ss,replace = FALSE)
     }) %>% bind_rows() %>% select(-nums)

  missing_data <- sampled_data1 %>% dplyr::filter(is.na(patid)) %>% 
    group_by(yob,gender) %>% summarise(nums=n(),.groups="drop")

  sampled_data <- missing_data %>%
    left_join(possibles, by = c("yob","gender")) %>%
    group_split(yob, gender) 

  sampled_data2 <- lapply(sampled_data,function(x){
    ss <- x %>% pull(nums) %>% unique()
    slice_sample(x, n=5*ss,replace = FALSE)
     }) %>% bind_rows() %>% select(-nums)

  sampled_data2 %>% dplyr::filter(is.na(patid))
  rm(possibles)
  gc()

  control_data <- bind_rows(sampled_data1,sampled_data2) %>% dplyr::filter(!is.na(patid))
  
  cases_sql <- str_c("
    SELECT DISTINCT
      pat.patid,
      pat.gender,
      pat.yob,
      pat.pracid,
      pra.region
    FROM
      acceptable_patients AS pat
    INNER JOIN 
      exclude AS ic
      ON ic.patid=pat.patid
    INNER JOIN
      practices AS pra
      ON pra.pracid=pat.pracid
   INNER JOIN
     linkages AS lnk
     ON lnk.patid=pat.patid
   WHERE
     lnk.hes_apc_e LIKE '1' AND
     lnk.ons_death_e LIKE '1' AND
     lnk.lsoa_e LIKE '1' AND
     lnk.hes_op_e LIKE '1' AND
     lnk.hes_ae_e LIKE '1'")
  
  cases_data <- get_table(dbf=dbn,sqlstr=cases_sql)

  all_patients <- list(case=cases_data,control=control_data) %>% plyr::ldply(.id="cohort") %>% tibble()
  ret <- all_patients %>% nrow()
  load_table(dbf=dbn,dataset=all_patients,tab_name="sample_cohort")

  return(ret)
}

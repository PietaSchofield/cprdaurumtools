# get observations batch
#'
#' get the drugissue records and convert some fields to useful field types
#'
#' @param bdir the name of the batch subdirectory
#' @param odir the name of the output directory
#' @param olist the list of observation codes
#' @param bpp BiocParallal Multicore Parameters
#'
#'
#' Pass a table of covariate codes and generate covariates table
#' @import magrittr
#' @export
load_probs <- function(pddir,dbf,ow=F,db=F,tab_name="problems",
    selvars=c("patid","obsid","parentprobobsid","probenddate","expduration","lastrevdate",
              "parentprobrelid","probstatusid")){
  obsfiles <- list.files(pddir,pattern="Prob.*txt$",full=T,recur=T)
  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  nrec <- 0
  if(!tab_name%in%duckdb::dbListTables(dbi) || ow){
    if(tab_name%in%duckdb::dbListTables(dbi)){
      duckdb::dbExecute(dbi,paste0("DROP TABLE ",tab_name,";"))
    }
    nrec <- lapply(obsfiles,function(fn){
      dat <- readr::read_tsv(fn,col_types=readr::cols(.default=readr::col_character())) %>%
        dplyr::select(dplyr::all_of(selvars)) %>%
        dplyr::mutate(probeddate = format(lubridate::dmy(probenddate)),
                      lastrevdate = format(lubridate::dmy(lastrevdate)))
      if(tab_name%in%duckdb::dbListTables(dbi)){
        app=T
        ovr=F
      }else{
        app=F
        ovr=T
      }
      duckdb::dbWriteTable(dbi,tab_name,dat,overwrite=ovr,append=app)
      nr <- dat %>% nrow()
      cat(paste0(basename(fn),": ",nr," records loaded\n"))
      rm(dat)
      gc()
      return(nr)
    })
  }
  duckdb::dbDisconnect(dbi)
  trec <- sum(unlist(nrec))
  return(cat(paste0(trec," records processed\n")))
}

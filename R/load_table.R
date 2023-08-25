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
load_table <- function(filename,dbf,ow=F,db=F,tab_name=gsub("[.].*","",basename(filename)), 
                       selvars=NULL){
  nrec <- 0
  if(file.exists(filename)){
    dbi <- RSQLite::dbConnect(RSQLite::SQLite(),dbf)
    if(!tab_name%in%RSQLite::dbListTables(dbi) || ow){
      if(tab_name%in%RSQLite::dbListTables(dbi)) RSQLite::dbExecute(dbi,paste0("DROP TABLE ",tab_name))
      dat <- readr::read_tsv(filename,col_types=readr::cols(.default=readr::col_character())) 
      if(!is.null(selvars)) dat <- dat %>% dplyr::select(dplyr::all_of(selvars)) 
      RSQLite::dbWriteTable(dbi,tab_name,dat,overwrite=T,append=F)
      nrec <- dat %>% nrow()
      cat(paste0(basename(filename),": ",nrec," records loaded\n"))
      rm(dat)
      gc()
    }else{
      cat(paste0(tab_name," exists\n"))
    }
    RSQLite::dbDisconnect(dbi)
  }else{
    cat(paste0(filename," not found\n"))
  }
  return(cat(paste0(nrec," records processed\n")))
}

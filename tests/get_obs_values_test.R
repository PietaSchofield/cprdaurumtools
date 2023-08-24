




bpp <-BiocParallel::SerialParam()
obslist <- file.path(rdir,"Aurum_diabetes_type_2_codes.txt") %>% 
readr::read_tsv(col_types=readr::cols(.default=readr::col_character())) %>%
     dplyr::select(MedCodeId) %>% dplyr::filter(!is.na(MedCodeId)) %>% dplyr::pull(MedCodeId) 
obslist <- ct
cvsetname="bmi"
cvset <- code_dict[[cvsetname]]
cvset

check <-  obsval %>% pull(check)
minv <- obsval %>% pull(min)
maxv <- obsval %>% pull(max)

res <- readRDS(obsrdsfile)
if(is.na(check)){
  res <-  res %>% dplyr::filter(value>=minv & value<=maxv)
}else{
  res <- res 
}
res

library(gt)
library(tidyverse)
library(DBI)
library(odbc)
library(lubridate)
library(readxl)


#Read Data from DB
myconn <- DBI::dbConnect(odbc::odbc(), "Snowflake")
prices <- DBI::dbGetQuery(myconn,"SELECT * FROM SBX_BIZ.MERCHANDISING.PPA_EXPO_PRICES")
products2 <- DBI::dbGetQuery(myconn,"SELECT * FROM MERCHANDISING.PPA_EXPO_PRODUCTS")

#This contains manual revisions that may not be captured in our dataset
products <- read_excel("Products711.xlsx")

products <- products %>% left_join(products2 %>% select("Product ID", "EVENT_START_DATE", "EVENT_END_DATE"))

showbook <- products %>% left_join(prices)


gt_theme_aidan <- function(data,...) {
  data %>%
    opt_all_caps()  %>%
    opt_table_font(
      font = list(
        google_font("Arimo"),
        default_fonts()
      )
    ) %>%
    tab_style(
      style = cell_borders(
        sides = "bottom", color = "transparent", weight = px(2)
      ),
      locations = cells_body(
        columns = TRUE,
        rows = nrow(data$`_data`)
      )
    )  %>% 
    tab_options(
      column_labels.background.color = "white",
      table.border.top.width = px(3),
      table.border.top.color = "transparent",
      table.border.bottom.color = "transparent",
      table.border.bottom.width = px(3),
      column_labels.border.top.width = px(3),
      column_labels.border.top.color = "transparent",
      column_labels.border.bottom.width = px(3),
      column_labels.border.bottom.color = "black",
      data_row.padding = px(3),
      source_notes.font.size = 12,
      table.font.size = 16,
      heading.align = "left",
      ...
    ) 
}

#flat file from T_WHSE_SHIPPER_CMPNTS
Components<- read_excel("comps.xlsx")
names(Components)[4] <- "Code"
names(Components)[1] <- 'Warehouse ID'






showbook <- showbook %>% mutate(Line1 = ifelse(Catalog %in% c("2023 GFD Food Show/Expo- Fall/Holiday",
"Meat Food Show/Expo- Fall/Holiday",
"Produce Food Show/Expo- Fall/Holiday",
"Deli Food Show/Expo- Fall/Holiday",
"Bakery Food Show/Expo- Fall/Holiday",
"2023 July Expo - GDF/GM/HBC Auction / Showstopper",
"GFD Food Show/Expo- Spring/Summer",
"2023 July Expo - GM/HBC Ownbrands"), 1, 0)) 




showbook <- showbook  %>% arrange(desc(Catalog), EVENT_START_DATE, UPC, Brand)






showbook$ITEM_NBR = str_split_fixed(showbook$`Product ID`, '-', 3) %>% 
  as.data.frame() %>% mutate(I = as.numeric(V2))

showbook$ITEM_NBR <- showbook$ITEM_NBR$I

showbook <- showbook %>% mutate(CatName =gsub("[^a-zA-Z]", "", showbook$`Extra 4`))



showbook <- showbook %>% filter(`Active?`== 'Y')  %>% filter(Line1 == 1) %>% 
  group_by(Catalog, UPC, Brand, EVENT_START_DATE, `Delivery Group`, Pack) %>% 
  mutate(ROW = cur_group_id()) %>% ungroup() %>% mutate("Row ID" = paste0(CatName, ROW))







#Function Start

#Pass in Catalog Name and Fac ID

printShowbook <- function(CatalogName, FacilityID){
  
  if (FacilityID == 1){
    components <- Components %>% mutate(Code=  as.numeric(Components$Code)) %>% filter(`Warehouse ID` == 1)
  }else{
    components <- Components %>% mutate(Code=  as.numeric(Components$Code)) %>% filter(`Warehouse ID` != 1)
  }
  
  
  
  show <- showbook %>% filter(`Active?` == 'Y')  %>% 
    filter(grepl(CatalogName, Catalog)) %>% 
    filter(`Warehouse ID` == FacilityID ) %>% 
    mutate("Row ID" = paste0(CatName, ROW)) %>% 
    select( `Row ID`, ITEM_NBR,UPC, Description, Pack,  Size, Category, Brand,
            `Total Price`,  `Show Price (Zone 1)`, Allowance,
            Catalog, `Delivery Group`,    EVENT_START_DATE, EVENT_END_DATE, CatName )
  
  
  names(show)[c(9, 10,11, 13)] <- c( "List Price",  "Show Price",  "Allow", "Deal Name")
  
  show$`Show Price` <- as.numeric(show$`Show Price`)
  
  show$`List Price` <- as.numeric(show$`List Price`)
  show<- show %>% select(-Category) %>% mutate("Deal Unit Cost"= `Show Price`/ Pack, 
                                               "Unit Cost" = `List Price`/ Pack)
  
  CatName <- show$CatName[1]
  
  
  show$`Unit Cost` <- as.numeric(show$`Unit Cost`)
  
  show$Allow <- as.numeric(show$Allow)

  
  
  
  names(show)[c(2,13,14)] <- c("Code","Deal Start", "Deal End")
  show <- show %>% select( -`Deal Name`, -CatName)
  show <- show %>% 
    mutate(`Deal Start` = paste0( month(`Deal Start`), "/", day(`Deal Start`), "/", year(`Deal Start`)),
           `Deal End` = paste0( month(`Deal End`), "/", day(`Deal End`), "/", year(`Deal End`)))
  
  
  show <- show %>% distinct() %>% arrange(Brand, Code, `Deal Start`) 
  

  
  

  comps <- show %>% left_join(components) %>% filter(!is.na(RETAIL_ITEM_DESC)) %>% 
    select(`Row ID`, Code, Brand,  COMP_UPC_UNIT, RETAIL_ITEM_DESC, QTY_IN_SHIPPER, 
           SIZE, `List Price`,   Allow, `Show Price`, `Unit Cost`, `Deal Unit Cost`, `Deal Start`, `Deal End`)
  
  
  names(comps)[4:7]<-c("UPC", "Description", "Pack", "Size")
  
  comps <- comps %>% select(`Row ID`, Code, Brand, UPC, Description, Pack, Size)
  comps$UPC <- as.numeric(comps$UPC)
  
  comps <- comps %>% left_join(show, by=c("Row ID")) %>% select(`Row ID`, Code.x, Brand.x, UPC.x, Description.x,
                                                                Pack.x, Size.x, UPC.y, Description.y)
  
  
  
  
  names(comps) <- c("Row ID", "Code", "Brand", "UPC", "Description", "Pack", "Size", 
                    "MasterUPC", "MasterDescription")
  
  
  # get number of row ids by UPC
  rowids <- comps %>% group_by(MasterUPC, Brand) %>% select(`Row ID`, Brand) %>% distinct() %>% 
    summarize(`Row ID` = `Row ID`, Brand= Brand, Count = n()) %>% 
    mutate(ID = ifelse(Count ==2, seq(1, 2), ifelse(Count ==3, seq(1, 3), 1)))%>% 
    pivot_wider(names_from = c("ID"), values_from = "Row ID")  %>% ungroup()
  
  if (nrow(comps)!=0){
    if(ncol(rowids)>4){
      rowids <- rowids %>% select(-Count) %>% mutate(`2` = ifelse(is.na(`2`), "", `2`))
      
      comps <- comps %>% left_join(rowids)  %>% mutate(gname = paste0(Brand, " - ",  MasterUPC
                                                                      ," - " ,MasterDescription)) %>% 
        select(gname, UPC, Description, Pack, Size) %>% distinct()
    } else {
      rowids <- rowids %>% select(-Count) %>% mutate(`1` = ifelse(is.na(`1`), "", `1`))
      
      comps <- comps %>% left_join(rowids)  %>% mutate(gname = paste0(Brand, " - ",  MasterUPC
                                                                      ," - " ,MasterDescription)) %>% 
        select(gname, UPC, Description, Pack, Size) %>% distinct()
      
    } 
  }
  
  
  t <- show[1, ] %>% gt %>% tab_header(title= 'SpartanNash Food Solutions Expo 2023', subtitle = paste0(CatalogName ,'-', CatName)) %>% 
    gt_theme_aidan() %>% tab_options(data_row.padding =px(1), 
                                     table.width = '98%', heading.align = 'center',
                                     heading.subtitle.font.size = 28, 
                                     heading.title.font.size = 38, column_labels.font.size =  0,
                                     table.font.size = 0, row_group.font.size = 0)
  
  t %>% gtsave("title.pdf")
  
  n <- nrow(show)
  
  j <- 19
  loops <- ceiling(n/j) -1
  i <- 19
  
  if(nrow(show[1:j,] %>% select("Brand") %>% distinct()) > 2){
    j <- 19
    
    i <- j
  }else if (nrow(show[(i+1):(i+j),] %>% select("Brand") %>% distinct()) == 1) {
    j <- 19
    
    i <- j
  }else {
    j <- 19
    
    i <- j
    
  }
  

  show <- show %>% select("Row ID", "Code", "UPC", "Description", "Pack", "Size", "Brand", 
                          "List Price", "Allow", "Show Price", "Unit Cost", "Deal Unit Cost", "Deal Start",
                          "Deal End")
  
  
  
  if(nrow(comps)!=0){
    comps <- comps %>% mutate(gname = gsub("NULL", "", gname)) %>%
      mutate(gname = gsub("c\\(", "", gname))      %>%
      mutate(gname = gsub("\\)", "", gname)) %>% 
      mutate(gname = gsub('"', "", gname))
    
  }
  
  
  
  show <- show %>% select(-`Row ID`)

  t <- show[1:i, ] %>% na.omit() %>%  arrange(Brand, Code, `Deal Start`) %>% gt(groupname_col = "Brand") %>% 
    tab_header(title = "SpartanNash Food Solutions Expo 2023", subtitle = paste0(CatalogName, '-', CatName)) %>% 
    fmt_currency(columns = c( "Show Price", "Unit Cost", "Allow", "Deal Unit Cost", 'List Price'), currency= "USD") %>%
    gt_theme_aidan() %>% tab_options(data_row.padding =px(4), 
                                     table.width = '100%', heading.align = 'center',
                                     heading.subtitle.font.size = 10, 
                                     heading.title.font.size = 14, column_labels.font.size =  10,
                                     table.font.size = 9, row_group.font.size = 10)
  t %>% gtsave("showbook.pdf")
  fileinputs <- "showbook.pdf"
  
  
  
  
  if(loops != 0){
    for(n in 1:loops)
    {  
      
      
      if(nrow(show[(i+1):(i+j),] %>% select("Brand") %>% distinct()) > 2){
        j <- 19
        t <- show[(i+1):(i+j), ] %>% na.omit() %>% arrange(Brand, Code, `Deal Start`) %>% gt(groupname_col = "Brand") %>% 
          fmt_currency(columns = c( "Show Price", "Unit Cost", "Allow", "Deal Unit Cost", 'List Price'), currency= "USD") %>%
          gt_theme_aidan() %>% tab_options(data_row.padding =px(4), 
                                           table.width = '100%', heading.align = 'center',
                                           heading.title.font.size = 14, column_labels.font.size =  10,
                                           table.font.size = 9, row_group.font.size = 10)
        
        i <- i+ j
      }else if (nrow(show[(i+1):(i+j),] %>% select("Brand") %>% distinct()) == 1) {
        j <- 19
        t <- show[(i+1):(i+j), ] %>% na.omit() %>% arrange(Brand, Code, `Deal Start`) %>% gt(groupname_col = "Brand") %>% 
          fmt_currency(columns = c( "Show Price", "Unit Cost", "Allow", "Deal Unit Cost", 'List Price'), currency= "USD") %>%
          gt_theme_aidan() %>% tab_options(data_row.padding =px(4), 
                                           table.width = '100%', heading.align = 'center',
                                           heading.title.font.size = 14, column_labels.font.size =  10,
                                           table.font.size = 9, row_group.font.size = 10)
        i <- i+j
      }else  {
        j <- 19
        t <- show[(i+1):(i+j), ] %>% na.omit() %>% arrange(Brand, Code, `Deal Start`) %>% gt(groupname_col = "Brand") %>% 
          fmt_currency(columns = c( "Show Price", "Unit Cost", "Allow", "Deal Unit Cost", 'List Price'), currency= "USD") %>%
          gt_theme_aidan() %>% tab_options(data_row.padding =px(4), 
                                           table.width = '100%', heading.align = 'center',
                                           heading.title.font.size = 14, column_labels.font.size =  10,
                                           table.font.size = 9, row_group.font.size = 10)
        i <- i+j
      }
      
      t %>% gtsave(paste0("showbook", n,".pdf"))
      fileinputs <- c(fileinputs, paste0("showbook", n,".pdf") )
      
      
      
    }
  }
  
  
  qpdf::pdf_combine(input = fileinputs,
                    output = "output.pdf")
  
  
  
  

  if(nrow(comps)!=0){
    
    #comps <- comps %>% select(-`Warehouse ID`)
    
    c <- comps  %>% arrange(UPC) %>% gt(groupname_col = "gname") %>%
      tab_header(title = "SpartanNash Food Solutions Expo 2023 Components", subtitle = paste0(CatalogName, '-', CatName)) %>%
      gt_theme_aidan() %>% tab_options(data_row.padding =px(4),
                                       table.width = '98%', heading.align = 'center',
                                       heading.subtitle.font.size = 10,
                                       heading.title.font.size = 14, column_labels.font.size =  10,
                                       table.font.size = 9, row_group.font.size = 10)
    
    
    c %>% gtsave("components.pdf")
  }
  # 
  # qpdf::pdf_combine(input = fileinputs,
  #                   output = "output2.pdf")
  CatalogName <- gsub("/", "-", CatalogName)
  #CatalogName <- "2023 July Expo - Pet Catalog"
  
  if(nrow(comps)!=0){
    qpdf::pdf_combine(input = c( "title.pdf","output.pdf", "components.pdf" ),
                      output = paste0(CatalogName, '-', FacilityID, ".pdf"))
    
  }else {
    qpdf::pdf_combine(input = c( "title.pdf","output.pdf"),
                      output = paste0(CatalogName, '-', FacilityID, ".pdf"))
    
  }
  
  c2 <- comps
  names(c2)[1] <- "Master Item"
  
  #library(xlsx)
  xlsx::write.xlsx(show, paste0(CatalogName, '-', FacilityID, '.xlsx'), sheetName = "Products", col.names = TRUE,showNA=TRUE,append=TRUE)
  xlsx::write.xlsx(c2 , paste0(CatalogName,'-', FacilityID, '.xlsx'), sheetName = "Components", col.names = TRUE,showNA=TRUE,append=TRUE)
}






#Other Catalogs -- waiting on updates

# "2023 GFD Food Show/Expo- Fall/Holiday",
# "Meat Food Show/Expo- Fall/Holiday",
# "Produce Food Show/Expo- Fall/Holiday",
# "Deli Food Show/Expo- Fall/Holiday",
# "Bakery Food Show/Expo- Fall/Holiday",
# "2023 July Expo - GDF/GM/HBC Auction / Showstopper",
# "GFD Food Show/Expo- Spring/Summer",
# "2023 July Expo - GM/HBC Ownbrands"


FacID = c(3,40,58,8,15, 1)



# Uncomment and Run this
# for (i in FacID){
#   printShowbook("2023 GFD Food Show/Expo- Fall/Holiday", i)
#   printShowbook("Meat Food Show/Expo- Fall/Holiday", i)
#   printShowbook("Produce Food Show/Expo- Fall/Holiday", i)
#   printShowbook("Deli Food Show/Expo- Fall/Holiday", i)
#   printShowbook("Bakery Food Show/Expo- Fall/Holiday", i)
#   printShowbook("2023 July Expo - GDF/GM/HBC Auction / Showstopper", i)
#   printShowbook("GFD Food Show/Expo- Spring/Summer", i)
#   printShowbook("2023 July Expo - GM/HBC Ownbrands", i)
# }







###GM HBC Catalogs -- DONE
# "2023 July Expo - Pet Catalog",
# "2023 July Expo - GM/HBC Ownbrands",
# "2023 July Expo - GM/HBC Showbook",
# "Spring and Summer Hardgoods",
# "Living Well",
# "Fashion",
# "Dollar",
# "4th Quarter Great Buys",
# "Indoor Pest Control",
# "Spring Cleaning",
# "Easter",
# "Summer Celebrations"



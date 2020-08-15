import requests
import re
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import lxml
import time
from tqdm import tqdm
import random

#####################################
####### HARD DRIVE LOCATION #########
#####################################

path = input("Provide the hard drive location you want to use for saving and reading files:")

#####################################
######## SCRAPING BOX OFFICE ########
#####################################

#fetch entire box office from .... until ....#
base_url_box_office = "https://www.boxofficemojo.com/year/"
loop_year = int(input("I want to scrape the Box Office FROM: "))
end_year = int(input("I want to scrape the Box Office UNTIL: "))
all_years = list(range(loop_year,end_year+1)) # We create a list of years we'll need to scrape (see further). We add one to the end year because the range function doesn't include the upper limit of the range#

#create empty dataframe for storing Box Office tables & empty list to fetch list of movie URLs#
boxoffice_mastertable = pd.DataFrame() #we'll save the Box Office table(s) in here#
linklist = list() #we'll add the links to the individual Box Office Mojo pages to this list and add it as a column to the Box Office table later on#

#settings for progress bar#
print()
pbar = tqdm(total=100) #create a progress bar with a maximum value of 100#
start = loop_year #we'll use the start and end year to update the progress bar#
end = end_year

#Now let's do the actual scraping! Fetch all Box Office tables using a while loop#
while loop_year < (end_year + 1): #as long as ( = while) the 'end year + 1' (2020) is bigger than the year we're looping right now, do the following:#

    #extract one specific Box Office table#
    url = base_url_box_office + str(loop_year)
    boxoffice_table = pd.read_html(url)[0] #The pandas library allows you to fetch tables (embedded in table HTML tags) from any webpage. We'll like to fetch the first table on the page (numbering in Python starts at zero, by the way)#
    boxoffice_table = boxoffice_table.drop(['Genre', 'Budget', 'Running Time','Estimated'], axis=1) #There are some empty variables present here, let's drop them#
    boxoffice_table['year'] = loop_year #create a new column in this table and fill it with the year you're scraping right now#

    #extract links of individual Box Office Mojo pages from Box Office table#
    page = requests.get(url, headers={'User-Agent': 'Chrome/79.0.3945','Connection':'close'})
    bsObj = BeautifulSoup(page.text, "html.parser");
    pattern = re.compile(r"^(?:[^\/]*\/){2}([^\/]*)\/.*$") #This is a REGEX pattern. We want to scrape the Box Office Mojo ID, which is present between the second and third '/' in the href links: '/release/rl3059975681/.ref=...'#

    for link in bsObj.find_all('a', href=True): #for all 'a' tags...#
        specificlink = link.get('href') #get me the 'href' attribute (i.e. the links)#
        if "/release/r" in specificlink: #if you can find the words '/release/r' in this link...#
            specificlink_clean = pattern.match(specificlink).group(1) #extract the Box Office Mojo ID by matching it with the Regex pattern defined before.#
            linklist.append(specificlink_clean) #and append that URL to our linklist#

    boxoffice_mastertable = boxoffice_mastertable.append(boxoffice_table, ignore_index=True, sort = False) #append the Box Office table to a general table containing the figures of *all* years#

    #update loop and progress bar
    loop_year = loop_year + 1 #Ok, we're done! Now we'll update the 'loop year' so that the while loop can start all over again.#
    pbar.update(100/(end-start+1)) #Let's update the progress bar. If you scrape ten years (e.g. 2010-2019), you'll want to update the progress bar with 10% or (100/(end-start+1))#

boxoffice_mastertable['link_movie'] = linklist #add all the links to the individual movie pages to our final Box Office table#

pbar.close() #close the progress bar#
time.sleep(2) #let the script pause for two seconds. The progress bar needs some time to close, after all. Otherwise, you run the risk that the bar keeps popping up...#
print()

print("I've finished scraping! Your box office table contains "+ str(len(boxoffice_mastertable)) + " rows") #Print this info message#
print()

limited_rows = input("Do you want to keep only a subset of the data (e.g. the top 50 films for each year)?(y/n)").lower() #ask the user if he/she wants to only keep a subset of the Box Office#
if limited_rows in ("y","yes","yup","yeah"): #if the input of limited rows was y/yes/yup/yeah, do the following:#
    delete = int(input("I only want to keep the top X (in ranking) of the Box Office:")) #ask the user which subset of the Box Office they want to keep#
    boxoffice_mastertable = boxoffice_mastertable[boxoffice_mastertable['Rank'] < (delete + 1)] #only keep Box Office rankings lower than the 'delete' number (+1)#

print("done!")

print()
print("writing to hard drive...")
boxoffice_hd_loc = path + "/BoxOfficeMojo.csv" #define the path for saving the file#
boxoffice_mastertable.to_csv(boxoffice_hd_loc, header = 'TRUE') #write the file#
print("done!")
print()

#####################################################
##### Prepare to scrape the different platforms #####
#####################################################

#create empty  dataframes so we can append the results of each year to a general dataset. We'll do this for each dataset we'll create#
imdb_ids_list_final = pd.DataFrame(columns=['link_boxofficemojo','link imdb'])
metacritic_ids_list_final = pd.DataFrame(columns=['link_imdb','link_metacritic'])
general_user_ratings_imdb_final = pd.DataFrame(columns=['imdb_url','agnostic_mean','median','weighted_rating','sample'])
socio_demographic_user_rating_imdb_final = pd.DataFrame(columns=['url','score','sample'])
us_vs_other_users_rating_imdb_final = pd.DataFrame(columns=['url','score','sample'])
metacritic_reviews_final = pd.DataFrame(columns=['url','weighted score','outlet','score'])

# setting headers for the browser. This is one of the little tricks we'll use to trick the platform into thinking we're a 'real' user#
headers_browser = {'Connection': 'close',"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
"Accept-Encoding": "gzip","Accept-Language": "en-US,en;q=0.9,es;q=0.8", "Upgrade-Insecure-Requests": "1","User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36"}

#######################################
##### scrape results for each year#####
#######################################

for year in all_years: #This is a giant loop. 90% of the script is basically embededd within this loop. So, for all Box Office years we've scraped in the previous step, do the following:#
    print("*****SCRAPING INFORMATION ON THE " + str(year) + " BOX OFFICE...*****") #Announce the user you'll be scraping a specific year#
    print()
    year_table_subset = boxoffice_mastertable[boxoffice_mastertable['year'] == year] #subset the mastertable of the Box Office by only retaining movies from the year 'year'#

    #######################################
    ######## SCRAPING URLS to IMDB ########
    #######################################

    print("scraping URLS to IMDB...")
    links_elements = year_table_subset['link_movie']
    links = ("https://www.boxofficemojo.com/release/" + links_elements) #create links we'll need to scrape by adding the Box Office MOJO ID's we've scraped earlier to the URL#

    linklist_imdb = list() #create an empty list. We'll fill this list with IMDB ID's later on#

    # set empty string to pause the scraping process every X pages#
    scraped_batch = 0 #up until now, we've scraped zero links to IMDB#
    random_batch = random.randrange(20, 30) #set first random batch to scrape#

    pbar = tqdm(total=100) #create a progress bar with a maximum value of 100#

    for url in links: #for all elements in the 'links' list (i.e.  the individual movie pages on Box Office Mojo for a particular release year), do the following:#
        for i in range(3): #loop the try-part (i.e. opening the link) until it works, but only try it 3 times at most#
            try: #try the following:#
                random_sleep_link = random.uniform(30, 45) #sleep for a random chosen amount of seconds (between 30 and 45)#
                time.sleep(random_sleep_link)
                page = requests.get(url,headers= headers_browser) #access the URL using the header settings defined earlier#

            except requests.exceptions.RequestException as e: #if anything weird happens...#
                print("\n I've encountered an error! I'll pause for five minutes and try again \n")
                time.sleep(300) #sleep the script for 300 seconds and....#
                continue #...start the loop again from the beginning#

            else: #if the try-part works eventually...#
                break #...break out of the loop#

        else: #if x amount of retries on the try-part don't work...#
            raise Exception("Something really went wrong here... I'm sorry.") #...raise an exception and stop the script#

        #if the script survived this part...#
        bsObj = BeautifulSoup(page.text, "html.parser") #this means we're good to go and can parse the page into BeautifulSoup!#
        pattern = re.compile('tt\d+') #REGEX pattern. We're looking for IMDB ID's, which all start with a 'tt' number. \d+ simply means 'any amount of numbers'#
        match = 0 #up until now, we have zero IMDB ID's#

        for link in bsObj.find_all("a"): #for all elements with an 'a' tag on the page, do the following:#
            if "Cast information" in link.text: #if "Cast information" appears in the text properties of the tag#
                imdb_link = link.get('href') #get me the href attribute (the URL)#
                imdb_number = re.search(pattern,imdb_link).group(0) #within that href,extract the pattern we've determined before (i.e. tt+numbers). This is the IMDB ID!#
                linklist_imdb.append(imdb_number) #append this IMDB ID to our list#
                match = 1 #We have our match, so set the match value to 1#
                if match == 1: #if we have a match...#
                    break #break the for loop of all the 'a' tags. We need this 'match' part because there are *multiple* references to the IMDB ID present on the page. We'll just need the first match.#

        scraped_batch = scraped_batch + 1 #We've scraped one extra link, so let's update our 'scraped batch' value#
        pbar.update(100 / len(links)) #Update our progress bar.#
        if scraped_batch == random_batch: #if (and only if!) our scraped batch has reached the treshold of our random batch (which we've determined earlier, between 30 and 60 links#
            random_pause_batch = random.uniform(45,120) #let the script pause between 45 and 120 seconds. This is one additional trick to prevent the platform of blocking our scraper#
            time.sleep(random_pause_batch)
            scraped_batch = 0 #reset our batch counter#
            random_batch = random.randrange(20,30) #and determine a new random batch of URLs#

    pbar.close() #close the progress bar#
    time.sleep(3) #let the script pause for two seconds. The progress bar needs some time to close, after all. Otherwise, you run the risk that the bar keeps popping up...#
    print()

    dict_imdb_links = {'link_boxofficemojo': links_elements, 'link imdb': linklist_imdb} #create a dictionary of variables...#
    imdb_ids_list = pd.DataFrame(dict_imdb_links) #which we'll use to create a dataframe containing the ID's of Box Office Mojo Pages and IMDB pages#
    imdb_ids_list_final = pd.concat([imdb_ids_list_final,imdb_ids_list]) #add these table to a general table containing all links between Box Office Mojo - IMDB#

    #########################################################
    ######## SCRAPING URLS to Metacritic (from IMDB) ########
    #########################################################

    print("scraping URLS to Metacritic using the IMDB links...")
    links = ("https://www.imdb.com/title/"+ pd.Series(linklist_imdb) + "/criticreviews") #create links we'll need to scrape by adding the IMDB ID's we've scraped earlier to the URL#

    linklist_metacritic = list()

    #settings for progress bar#
    pbar = tqdm(total=100)

    for url in links:
        for i in range(3): #loop the try-part (i.e. opening the link) until it works, but only try it 3 times at most#
            try: #try the following:#
                page = requests.get(url,headers= headers_browser) #access the URL using the header settings defined earlier#

            except requests.exceptions.RequestException as e: #if anything weird happens...#
                print("\n I've encountered an error! I'll pause for five minutes and try again \n")
                time.sleep(300) #sleep the script for 300 seconds and....#
                continue #...start the loop again from the beginning#

            else: #if the try-part works...#
                break #...break out of the loop#

        else: #if x amount of retries on the try-part don't work...#
            raise Exception("Something really went wrong here... I'm sorry.") #...raise an exception and stop the script#

        #if the script survived this part...#
        bsObj = BeautifulSoup(page.text, "html.parser") #this means we're good to go and can parse the page into BeautifulSoup!#
        pattern = re.compile('(?<=movie/)(.*)(?=\?ftag)') #REGEX pattern. We're looking for Metacritic ID's, which all appear in the URL after "movie/" the (?<=) and (?=) part are a lookbehind and lookahead respectively. The (.*) part simply means "whatever character(s) between the lookbehind and lookahead" #
        match = 0 #up until now, we have zero Metacritic ID's#

        for link in bsObj.find_all("a"): #for all elements with an 'a' tag on the page, do the following:#
            metacritic_id = str() #set metacritic id to empty each time you scrape a link. By doing so, we can catch when a specific page contains no link to Metacritic.
            empty_string = "" #define an empty string. We'll use this later#

            if "reviews on Metacritic.com" in link.text: #if "reviews on Metacritic.com" can be found in the text properties of the "a" tag, do the following:#
                metacritic_link = link.get('href') #get me the href attribute (i.e. the URL)#
                metacritic_id = re.search(pattern,metacritic_link).group(0) #within that href,extract the pattern we've determined before (i.e. everything between 'movie/' and 'ftag'). This is the Metacritic ID!#
                match = 1 #set the match value to 1#
                if match == 1: #as soon as we have a single match#
                    break #break the 'for' loop of all 'a' tags, since we have our Metacritic ID (this is not really necessary for the loop to work here, but it avoids unnecessary looping)#

        if metacritic_id == empty_string: #if we haven't found any "a" tag with the 'Reviews on Metacritic.com" text property, than our metacritic ID value remains empty#
            metacritic_id = "No metacritic reviews available" #if that's the case, simply fill in the metacritic id with "No metacritic reviews available"#

        linklist_metacritic.append(metacritic_id) #append the metacritic id to a general list containing all Metacritic id's#
        pbar.update(100/len(links)) #update our progress bar#

    pbar.close() #close the progress bar#
    time.sleep(3) #let the script pause for two seconds. The progress bar needs some time to close, after all. Otherwise, you run the risk that the bar keeps popping up...#
    print()

    dict_metacritic_ids_list = {'link_imdb': linklist_imdb, 'link_metacritic': linklist_metacritic} #create a dictionary of variables...#
    metacritic_ids_list = pd.DataFrame(dict_metacritic_ids_list) #which we'll use to create a dataframe containing the ID's of IMDB and Metacritic#
    metacritic_ids_list_final = pd.concat([metacritic_ids_list_final,metacritic_ids_list])  #add these table to a general table containing all links between IMDB-Metacritic#

    ############################################################
    ############ SCRAPE USER AND CRITICS RATING ################
    ############################################################

    ##############################
    ##### SCRAPE USER RATING #####
    ##############################

    ##############
    #preparations#
    ##############
    print("preparing to scrape user ratings...")
    print()

    #create links#
    links = ("https://www.imdb.com/title/"+ pd.Series(linklist_imdb) + "/ratings") #create links we'll need to scrape by adding the IMDB ID's we've scraped earlier to the URL#

    #prepare empty list for saving the general ratings#
    mean_list = list()
    median_list = list()
    weighted_rating_list = list()
    number_votes_list = list()

    #prepare an empty dataframe to save the ratings for each socio-demographic group + for the USA/Non-USA voters table#
    socio_demographic_user_rating_imdb = pd.DataFrame()
    us_vs_other_users_rating_imdb = pd.DataFrame()

    ##########
    #scraping#
    ##########

    print("scraping user ratings on IMDB")

    #settings for progress bar#
    pbar = tqdm(total=100) #Create a progress bar with a maximum value of 100#

    for url in links: #for all links, do the following:#

        #1.scrape the general user ratings#
        ###################################
        page = requests.get(url,headers= headers_browser) #acces the page#
        bsObj = BeautifulSoup(page.text, "html.parser") #parse the page into a beautifulsoup object#

        text_ratings = bsObj.find('div',{'align': 'center'}).text.strip() #target a specific div tag which is aligned in the center of the page#
        raw_mean = re.search(re.compile("(?<=mean = )(.*)(?=\n)"),text_ratings).group(0) #extract the arithmetic mean using a REGEX pattern from this specific div#
        raw_median = re.search(re.compile("(?<=Median = )(.*)"),text_ratings).group(0) #extract the arithmetic median using a REGEX pattern from this specific div#

        text_weighted_rating = bsObj.find('div', class_='inline-block ratings-imdb-rating').text.strip() #target a specific div tag with a specific class attribute, containing the wieghted average calculated by the IMDB algorithm#
        number_votes = bsObj.find('div', class_='allText').text #target a specific div tag with a specific class attribute, containing the sample size#
        number_votes = re.search(re.compile('(.*)(?=\\nIMDb users have given)'),number_votes).group(0).strip() #extract the sample size using a REGEX pattern from this specific div#

        mean_list.append(raw_mean)#append the mean to a general list containing all means. Do the same for the median weighted average and sample size#
        median_list.append(raw_median)
        weighted_rating_list.append(text_weighted_rating)
        number_votes_list.append(number_votes)

        #2. scrape the table of user rating by socio-demographic group#
        ###############################################################
        table = pd.read_html(url)[1] #the averages by socio-demograpgic group is embedded within a table tag, so we can simply use the Pandas library to extract the table. We want the *second* table on the page, so we select [1] (counting in Python starts at zero, by the way.)#
        table = pd.melt(table) #melt the table. Melting a dataframe means that each and every row/column combo becomes its own row, so we basically expand the table here#
        table = table.drop(table.index[0:4]) #let's drop some superfluous rows#
        table = table.drop(['variable'], axis=1) #drop the 'variable' column, which contains the different categories, but the names of the categories are not specific enough#
        table['category'] = ['male', 'female', '< 18 general', '< 18 male', '<18 female', '18-29 general', '18-29 male',
                             '18-29 female', '30-44 general', '30-44 male', '30-44 female', '45+ general', '45+ male',
                             '45+ female'] #create a new categories variable#
        table = table.set_index('category') #make this category variable the index of the dataframe (not really necessary), but it looks neat#
        table_splitted = table["value"].str.split(" ", n=1, expand=True) #the "value" column contains the score *and* the sample size in the same column. Let's split this columnn on the space character#
        table["score"] = table_splitted[0] #the score is the first element of the table we splitted (see previous line). So we select element [0]#
        table["sample"] = table_splitted[1] #the sample size is the second element#
        table = table.drop(['value'], axis=1) #we've stored the score and sample size into two new variables, so we can simply drop the old 'value' column containing both#
        table['url'] = url #add the IMDB url to the table of ratings#
        table = table[['url', 'score', 'sample']] #rearrange the table#
        table['sample'] = table['sample'].str.replace(",","") #remove (i.e. 'replace with nothing') the comma in the sample size column.#
        socio_demographic_user_rating_imdb = pd.concat([socio_demographic_user_rating_imdb, table]) #add all tables to a general table (for each year)#

        #3. scrape the table of user rating by non-USA versus USA voter#
        ################################################################
        table = pd.read_html(url)[2] #the rating of USA versus other users is embedded within the third table on the page, so we select table number [2] (remember that Python starts to count from zero)#
        table = pd.melt(table) #melt the table#
        table = table.drop(table.index[0]) #drop a superfluous row#
        table_splitted = table["value"].str.split(" ", n=1, expand=True) #perform the same split-trick again, just like we did when scraping the rating by socio-demographic group#
        table["score"] = table_splitted[0]
        table["sample"] = table_splitted[1]
        table = table.drop(['value'], axis=1)
        table['url'] = url
        table = table.set_index('variable')
        table = table[['url','score','sample']]
        table['sample'] = table['sample'].str.replace(",","") #again, remove the commas in the sample size#
        us_vs_other_users_rating_imdb = pd.concat([us_vs_other_users_rating_imdb, table]) #again, add all tables to a general table (for each year)#

        #update progress bar#
        pbar.update(100 / len(links)) #update the progress bar#

    pbar.close() #close the progress bar#
    time.sleep(3) #let the script pause for two seconds. The progress bar needs some time to close, after all. Otherwise, you run the risk that the bar keeps popping up...#
    print()

    #add the yearly datasets to a general dataset (containing all years)#
    #####################################################################
    dict_general_user_ratings_imdb = {'imdb_url': links, 'agnostic_mean': mean_list, 'median': median_list,
                                      'weighted_rating': weighted_rating_list, 'sample': number_votes_list}
    general_user_ratings_imdb = pd.DataFrame(dict_general_user_ratings_imdb)
    general_user_ratings_imdb['sample'] = general_user_ratings_imdb['sample'].str.replace(",", "")
    general_user_ratings_imdb_final = pd.concat([general_user_ratings_imdb_final,general_user_ratings_imdb])
    socio_demographic_user_rating_imdb_final = pd.concat([socio_demographic_user_rating_imdb_final,socio_demographic_user_rating_imdb], sort=False)
    us_vs_other_users_rating_imdb_final = pd.concat([us_vs_other_users_rating_imdb_final,us_vs_other_users_rating_imdb])

    print("done!")
    print()

    #################################
    ##### SCRAPE CRITICS RATING #####
    #################################

    ##############
    #preparations#
    ##############
    print("preparing to scrape critics ratings...")
    print()

    #create links#
    while "No metacritic reviews available" in linklist_metacritic: linklist_metacritic.remove("No metacritic reviews available") #remove movies without Metacritic reviews!#
    links = ("https://www.metacritic.com/movie/" + pd.Series(linklist_metacritic) + "/critic-reviews") #create links we'll need to scrape by adding the Metacritic ID's we've scraped earlier to the URL#

    #set empty string to pause the scraping process every X pages#
    scraped_batch = 0 #we've scraped zero pages up until now#
    random_batch = random.randrange(30,60)  #set first random batch between 30 and 60 URLs#

    ##########
    #scraping#
    ##########
    print("scraping critics rating on Metacritic \n")

    #settings for progress bar#
    pbar = tqdm(total=100) #Create a progress bar with a maximum value of 100#

    for url in links: #for all elements within the links series, do the following:#

        ###preparing lists for outlets, scores and titles###
        title_list = list()
        general_score_list = list()
        outlets_list = list()
        score_list = list()

        for i in range(3): # loop the try-part (i.e. opening the link) until it works, but only try it 3 times at most#
            try: #try the following:#
                random_sleep_link = random.uniform(30,45) #sleep for a random chosen amount of seconds between 30 and 45 seconds#
                time.sleep(random_sleep_link)
                page = requests.get(url,headers= headers_browser) #access the URL using the header settings defined earlier#

            except requests.exceptions.RequestException as e: #if anything weird happens...#
                print("I've encountered an error! I'll pause for five minutes and try again \n")
                time.sleep(300) #sleep the script for 300 seconds and....#
                continue #...start the loop again from the beginning#

            else: #if the try-part works...#
                break #...break out of the loop#

        else: #if x amount of retries on the try-part don't work...#
            raise Exception("Something really went wrong here... I'm sorry.") #...raise an exception and stop the script#

        # if the script survived this part...#
        bsObj = BeautifulSoup(page.text,"lxml") #this means we're good to go and can parse the page into BeautifulSoup!#

        ### scrape outlets and scores ###
        reviews = bsObj.find_all("div", {"class": "right fl"}) #target a specific div tag with a specific class. These divs contain all reviews!#

        #1.Scrape outlet#
        #################

        for element in reviews: #for every div element (read: for every review) within the reviews object#
            try: #try the following:#
                outlet = element.findChild("img") #try to find an image in the review containing the outlet name. The 'findChild' function means we're looking for a nested element here (an "img" tag *within* the "div" tag)#
                outlet = outlet['title'] #extract the title attribute from this image (which is the outlet name in *text*#
            except: #if you can't find an "img" 'child' in this div tag, do the following:#
                outlet = element.findChild("a") #if it doesn't find an image, the outlet is simply displayed as text and is contained within the first "a" tag#
                outlet = outlet.text #extract the text property from this a tag#
            finally:#whatever you've done before, do the following:#
                outlets_list.append(outlet) #add the outlet value to our outlet list#

        #2.Scrape score#
        ################

        ###scrape specific score of all outlets###
        scores = bsObj.find_all("div", {"class": re.compile("metascore_w large movie(.*)")}) #target a specific div with a class that contains the value "metascore_w large movie *and then something else*". Now we've scraped all the reviewers' scores.#
        for element in scores: #for each element (read: reviewer score), do the following:#
            score_list.append(element.text) #append the text property of this element to our score list#

        ###scrape general score provided by the Metacritic algorithm###
        general_score = bsObj.find("td", {"class": "num_wrapper"}).text #target a specific td tag (the td tag always represents a specific cell in a HTML table) with the class "num_wrapper"#
        general_score = re.sub(r"\n","",general_score) #remove the 'new line' HTML markup from the text#
        url_repetition = len(reviews) #repeat general score for the amount of reviews available#
        general_score_list.extend([general_score]*url_repetition) #add this general score x number of times to a general list#

        #Repeat title x number of times#
        url_repetition = len(reviews)  # one line for each review + 1 (because we also add the general score)#
        title_list.extend([url] * url_repetition) #similarly, add this title x number of times to a general list#

        #create dataframe and append it to general dataframe containing all movies#
        dict_Metacritic_reviews = {"url": title_list, "weighted score": general_score_list, "outlet": outlets_list,
                                       "score": score_list} #create a dictionary to define the variables of a dataframe. These variables consist of the list of scores we've just scraped#
        metacritic_reviews = pd.DataFrame(dict_Metacritic_reviews) #create the dataframe based on the dictionary.#
        metacritic_reviews_final = pd.concat([metacritic_reviews_final, metacritic_reviews]) #add this table to a general table containing the metacritic reviews of all movies (of all years)#


        scraped_batch = scraped_batch + 1 #We've scraped one extra link, so let's update our 'scraped batch' value#
        pbar.update(100 / len(links)) #update the progress bar#
        if scraped_batch == random_batch: #if (and only if!) our scraped batch has reached the treshold of our random batch (which we've determined earlier, between 30 and 60 links#
            random_pause_batch = random.uniform(30,60) #let the script pause between 30 and 60 seconds. This is one additional trick to prevent the platform of blocking our scraper#
            time.sleep(random_pause_batch)
            scraped_batch = 0 #reset our batch counter#
            random_batch = random.randrange(20,30)  #and determine a new random batch of URLs#

    pbar.close() #close the progress bar#
    time.sleep(3) #let the script pause for two seconds. The progress bar needs some time to close, after all. Otherwise, you run the risk that the bar keeps popping up...#
    print()

    if year != end_year:  # pause 30 minutes before next scraping session, but don't do it at the end of the scraping session (so only if the current year you're scraping is not the end year#
        print("Done! Pausing for 30 minutes before continuing with the next year of the Box Office \n")
        time.sleep(1500)
        print("Starting next scraping session in 5 minutes \n")
        time.sleep(240)
        print("1 minute.... \n")
        time.sleep(60)
        print()

#######################################################
######### Writing dataframes to hard drive ############
#######################################################

print("writing the general ratings of IMDB to hard drive... \n")
imdb_general_rating_path = path + "/IMDB_general_ratings.csv"
general_user_ratings_imdb_final.to_csv(imdb_general_rating_path, header='TRUE')
print("done! \n")

print("writing the ratings of IMDB by socio-demographic group to hard drive... \n")
imdb_socio_demographic_rating_path = path + "/IMDB_socio_demographic_ratings.csv"
socio_demographic_user_rating_imdb_final.to_csv(imdb_socio_demographic_rating_path, header='TRUE')
print("done!\n")

print("writing the ratings of IMDB by socio-demographic group to hard drive... \n")
imdb_us_versus_others_path = path + "/IMDB_us_versus_others.csv"
us_vs_other_users_rating_imdb_final.to_csv(imdb_us_versus_others_path, header='TRUE')
print("done!\n")

print("writing the critic ratings of Metacritic to hard drive... \n")
metacritic_rating_path = path + "/Metacritic_reviews.csv"
metacritic_reviews_final.to_csv(metacritic_rating_path, header='TRUE')
print("done! \n")
print()

print()
print("writing Metacritic ids to hard drive...\n")
Metacriticpages_hd_loc = path + "/Metacritic_page_url.csv"
metacritic_ids_list_final.to_csv(Metacriticpages_hd_loc, header='TRUE')
print("done! \n")
print()

print("writing IMDB ids to hard drive... \n")
Imdbpages_hd_loc = path + "/IMDB_page_url.csv"
imdb_ids_list_final.to_csv(Imdbpages_hd_loc, header='TRUE')
print("done!")
print()
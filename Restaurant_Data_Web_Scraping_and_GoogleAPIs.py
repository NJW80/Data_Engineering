import pandas as pd
import time
from nome_functions import nome_functions as nm


# *****************************************************************************
# Set general display options
# can be commented out when program is 'productionalised'
pd.options.display.float_format = '{:20,.2f}'.format
desired_width=350
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)
# *****************************************************************************


# *****************************************************************************
# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
# Step 1 - Get the number of restaurants in a city (London) and then divide by
# 100 to get the number of iterations needed to loop through all pages
# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -

# This function gets called from within the Get_Opentable_URLs function to get the number
# of restaurants and therefore the number of pages needed to loop through
# nm.Get_Opentable_City_Listing_Total() ---- Function call in here for demonstration purposes
# only. Don't comment back in to run


# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
# Step 2 - Loop through all pages for the city to get all restaurant Opentable
# URLs
# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -

cities = ['london']
Opentable_URLs = nm.Get_Opentable_URLs
Opentable_URLs = Opentable_URLs.drop_duplicates()
print("There are {} records in the Restaurant URL dataframe".format(len(Opentable_URLs.index)))
Opentable_URLs.to_pickle('Restaurant_URLs')
# Pickling and splitting of data due to whilst developing the program and also
# because it can take a long time to process 4k places


# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
# Step 3 - loop through the restaurant listing URLs to get the cuisine and
# google place id from the Opentable page for each restaurant
# need to sleep the calls to the Opentable website as there are c. 4k restaurants
# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -

Opentable_URLs = pd.read_pickle('Restaurant_URLs')

print(Opentable_URLs.head())
print("There are {} records in the Restaurant URL dataframe".format(len(Opentable_URLs.index)))

def get_Opentable_data_split(df, startpos, endpos, picklename):

    URLs_df = df.ix[startpos:endpos, :]
    print("There are {} records in the Restaurant URL dataframe".format(len(URLs_df.index)))

    start = time.time()
    print("Start time: {}".format(time.gmtime(start)))

    Opentable_Restaurant_Page_Data = pd.DataFrame(URLs_df.apply(lambda row: nm.get_Opentable_Restaurant_Data(row), axis=1)
                                                  .values.tolist(),columns=['Opentable_Cuisine','Opentable_Address']
                                                  ,index=URLs_df.index)

    Opentable_Restaurant_Data=pd.concat([Opentable_Restaurant_Page_Data,URLs_df],axis=1)
    Opentable_Restaurant_Data.to_pickle(picklename)

    stop = time.time()
    print("Finshing time: {}".format(time.gmtime(stop)))
    print("Loop duration: {}".format(stop - start))

get_Opentable_data_split(Opentable_URLs, 0, 499, 'Opentable_Restaurant_Data_0_499')
get_Opentable_data_split(Opentable_URLs, 500, 999, 'Opentable_Restaurant_Data_500_999')
get_Opentable_data_split(Opentable_URLs, 1000, 1499, 'Opentable_Restaurant_Data_1000_1499')
get_Opentable_data_split(Opentable_URLs, 1500, 1999, 'Opentable_Restaurant_Data_1500_1999')
get_Opentable_data_split(Opentable_URLs, 2000, 2499, 'Opentable_Restaurant_Data_2000_2499')
get_Opentable_data_split(Opentable_URLs, 2500, 2999, 'Opentable_Restaurant_Data_2500_2999')
get_Opentable_data_split(Opentable_URLs, 3000, 3499, 'Opentable_Restaurant_Data_3000_3499')
get_Opentable_data_split(Opentable_URLs, 3500, 3999, 'Opentable_Restaurant_Data_3500_3999')
get_Opentable_data_split(Opentable_URLs, 4000, 4499, 'Opentable_Restaurant_Data_4000_plus')


def get_GooglePlaces_Data(in_picklename, out_picklename):

    start = time.time()
    print("Start time of Google Data API loop: {}".format(time.gmtime(start)))

    Opentable_Restaurant_Data = pd.read_pickle(in_picklename)

    Google_ID_Address_Data = pd.DataFrame(Opentable_Restaurant_Data.apply(lambda row: nm.get_google_place_data(row), axis=1)
                                          .values.tolist(),columns=['Google_Name', 'Google_Place_ID', 'Google_Address']
                                          ,index=Opentable_Restaurant_Data.index)

    External_Restaurant_Data=pd.concat([Google_ID_Address_Data,Opentable_Restaurant_Data],axis=1)
    External_Restaurant_Data.to_pickle(out_picklename)
    print(External_Restaurant_Data.head())

    stop = time.time()
    print("Google Data API loop for {}".format(in_picklename))
    print("Finshing time of Google Data API loop: {}".format(time.gmtime(stop)))
    print("Loop duration: {}".format(stop - start))

get_GooglePlaces_Data('Opentable_Restaurant_Data_0_499', 'Google_Places_Data_0_499')
get_GooglePlaces_Data('Opentable_Restaurant_Data_500_999', 'Google_Places_Data_500_999')
get_GooglePlaces_Data('Opentable_Restaurant_Data_1000_1499', 'Google_Places_Data_1000_1499')
get_GooglePlaces_Data('Opentable_Restaurant_Data_1500_1999', 'Google_Places_Data_1500_1999')
get_GooglePlaces_Data('Opentable_Restaurant_Data_2000_2499', 'Google_Places_Data_2000_2499')
get_GooglePlaces_Data('Opentable_Restaurant_Data_2500_2999', 'Google_Places_Data_2500_2999')
get_GooglePlaces_Data('Opentable_Restaurant_Data_3000_3499', 'Google_Places_Data_3000_3499')
get_GooglePlaces_Data('Opentable_Restaurant_Data_3500_3999', 'Google_Places_Data_3500_3999')
get_GooglePlaces_Data('Opentable_Restaurant_Data_4000_plus', 'Google_Places_Data_4000_plus')




def get_GooglePlaces_Data(in_picklename, out_picklename):

    start = time.time()
    print("Start time of Google Data API loop: {}".format(time.gmtime(start)))

    Google_Restaurant_Data = pd.read_pickle(in_picklename)

    nm.write_places(data=Google_Restaurant_Data, tablename='google_place_data', replace_append='append')

    stop = time.time()
    print("Google Data API loop for {}".format(in_picklename))
    print("Finshing time of Google Data API loop: {}".format(time.gmtime(stop)))
    print("Loop duration: {}".format(stop - start))


get_GooglePlaces_Data('Opentable_Restaurant_Data_0_499', 'Google_Places_Data_0_499')
get_GooglePlaces_Data('Opentable_Restaurant_Data_500_999', 'Google_Places_Data_500_999')
get_GooglePlaces_Data('Opentable_Restaurant_Data_1000_1499', 'Google_Places_Data_1000_1499')
get_GooglePlaces_Data('Opentable_Restaurant_Data_1500_1999', 'Google_Places_Data_1500_1999')
get_GooglePlaces_Data('Opentable_Restaurant_Data_2000_2499', 'Google_Places_Data_2000_2499')
get_GooglePlaces_Data('Opentable_Restaurant_Data_2500_2999', 'Google_Places_Data_2500_2999')
get_GooglePlaces_Data('Opentable_Restaurant_Data_3000_3499', 'Google_Places_Data_3000_3499')
get_GooglePlaces_Data('Opentable_Restaurant_Data_3500_3999', 'Google_Places_Data_3500_3999')
get_GooglePlaces_Data('Opentable_Restaurant_Data_4000_plus', 'Google_Places_Data_4000_plus')



# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
# Saving the results
# Step 3 - Check data has been loaded correctly
# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -

External_Place_df = nm.read_places('google_place_data')
print(External_Place_df.head())




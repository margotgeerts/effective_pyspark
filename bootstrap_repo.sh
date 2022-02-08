rm -rf .git
git init
git add .
git rm --cached \
  tests/test_distance_metrics_solution.py \
  tests/comparers_solution.py \
  exercises/b_unit_test_demo/distance_metrics_corrected.py \
  exercises/c_labellers/dates_solution.py \
  exercises/d_laziness/test_improved.py \
  exercises/d_laziness/improved_date_helper.py \
  exercises/d_laziness/test_laziness.py \
  exercises/h_cleansers/clean_airports.py \
  exercises/h_cleansers/clean_carriers.py \
  exercises/h_cleansers/clean_flights.py \
  exercises/h_cleansers/test_clean_flights.py \
  exercises/h_cleansers/cleaning_villo_stations_solution.py \
  exercises/i_catalog/catalog.py \
  exercises/m_business/master_flights.py \
  exercises/m_business/num_flights.py \
  exercises/resources/fhvhv_tripdata_2020-05.csv

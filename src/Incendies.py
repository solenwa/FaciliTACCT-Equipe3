# NOT UP TO DATE

import pandas as pd

#ETL
# extract 
# transform
# load

def extract():
    """extract 2 raw fire csv and concat them"""
    filepath1 = "data/Incendies_2006_2016.csv"
    filepath2 = "data/Incendies_2016_2023.csv"
    dataframe1 = pd.read_csv(filepath1, sep=";", skiprows=6)
    dataframe2 = pd.read_csv(filepath2, sep=";", skiprows=3)
    df = pd.concat([dataframe1, dataframe2])
    return df

def transform(df):
    """clean CSV
    check if INSEE's code is correct
    filter useful columns
    convert area to km2
    and aggregate fires per year and countie
    """
    # To check that size of Code INSEE's values are always equal to 5
    longueursCodeInsee = df['Code INSEE'].astype(str).str.len()
    valeurs_uniques = longueursCodeInsee.unique()
    print(valeurs_uniques)

    # To convert m2 to km2
    df['Surface parcourue (km2)'] = df['Surface parcourue (m2)'] * 1e-6

    # To filter columns
    new_df = df[['Année', 'Code INSEE', 'Surface parcourue (km2)']].copy()
    # Number of fires per community
    df_incendies_par_collectivite_et_annee = new_df.groupby(['Code INSEE', 'Année']).count().reset_index().copy()
    df_incendies_par_collectivite_et_annee = df_incendies_par_collectivite_et_annee.rename(columns={'Surface parcourue (km2)': 'Nombre incendies'})

    # Area covered by fire, by year and by community
    df_surfaces_incendiées_par_collectivite_et_annee = new_df.groupby(['Code INSEE', 'Année'])['Surface parcourue (km2)'].sum().reset_index().copy()

    # To merge calculated values
    df_calcul_incendies_et_surfaces = pd.merge(df_incendies_par_collectivite_et_annee, df_surfaces_incendiées_par_collectivite_et_annee, on=['Code INSEE', 'Année'])
    return new_df, df_calcul_incendies_et_surfaces

def load(df, file_name):
    # Exporting dataframes
    df.to_csv(file_name, index=False)

def main():
    """generate aggregate fires data"""
    df = extract()
    new_df, df_calcul_incendies_et_surfaces = transform(df)

    load(new_df, "data/output/Incendies.csv")
    load(df_calcul_incendies_et_surfaces,'data/output/Incendies_calculs.csv')


if __name__ == "__main__":
    main()

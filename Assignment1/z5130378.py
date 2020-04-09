import ast
import json
import matplotlib.pyplot as plt
import pandas as pd
import sys
import os
import numpy as np

studentid = os.path.basename(sys.modules[__name__].__file__)


#################################################
# Your personal methods can be here ...

def read_csv(csv_file):
    return pd.read_csv(csv_file)

#################################################


def log(question, output_df, other):
    print("--------------- {}----------------".format(question))
    if other is not None:
        print(question, other)
    if output_df is not None:
        print(output_df.head(5).to_string())


def question_1(movies, credits):
    """
    :param movies: the path for the movie.csv file
    :param credits: the path for the credits.csv file
    :return: df1
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################

    movies = read_csv('movies.csv')
    credits = read_csv('credits.csv')

    df1 = pd.merge(movies, credits, how='inner', on='id')

    #################################################

    log("QUESTION 1", output_df=df1, other=df1.shape)
    return df1


def question_2(df1):
    """
    :param df1: the dataframe created in question 1
    :return: df2
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################

    df2 = df1[['id','title','popularity','cast','crew','budget','genres','original_language','production_companies','production_countries','release_date','revenue','runtime','spoken_languages','vote_average','vote_count']]
 
    #################################################

    log("QUESTION 2", output_df=df2, other=(len(df2.columns), sorted(df2.columns)))
    return df2


def question_3(df2):
    """
    :param df2: the dataframe created in question 2
    :return: df3
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################

    df3 = df2.set_index('id')

    #################################################

    log("QUESTION 3", output_df=df3, other=df3.index.name)
    return df3


def question_4(df3):
    """
    :param df3: the dataframe created in question 3
    :return: df4
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################

    df4 = df3[df3.budget != 0]

    #################################################

    log("QUESTION 4", output_df=df4, other=(df4['budget'].min(), df4['budget'].max(), df4['budget'].mean()))
    return df4


def question_5(df4):
    """
    :param df4: the dataframe created in question 4
    :return: df5
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################

    df4['success_impact'] = df4.apply(lambda row: (row.revenue - row.budget)/row.budget, axis=1)

    df5 = df4.copy()
    
    #################################################

    log("QUESTION 5", output_df=df5,
        other=(df5['success_impact'].min(), df5['success_impact'].max(), df5['success_impact'].mean()))
    return df5


def question_6(df5):
    """
    :param df5: the dataframe created in question 5
    :return: df6
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################

    df5["popularity"]=((df5["popularity"] - df5["popularity"].min())/(df5["popularity"].max() - df5["popularity"].min()))*100

    df6 = df5.copy()

    #################################################

    log("QUESTION 6", output_df=df6, other=(df6['popularity'].min(), df6['popularity'].max(), df6['popularity'].mean()))
    return df6


def question_7(df6):
    """
    :param df6: the dataframe created in question 6
    :return: df7
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################

    df7 = df6.astype({"popularity": 'int16'})
    
    #################################################

    log("QUESTION 7", output_df=df7, other=df7['popularity'].dtype)
    return df7

def question_8(df7):
    """
    :param df7: the dataframe created in question 7
    :return: df8
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    
    s1 = df7['cast'].str.extractall(r"('character': (['\"])(?:(?=(\\?)).)*?)\2")

    s1 = s1[0].apply(lambda x: x.replace("'character':", ''))

    s1 = s1.apply(lambda x: x.replace("'", ''))

    s1 = s1.apply(lambda x: x.replace("\"", ''))

    s1 = s1.sort_values(axis = 0)

    s1 = s1.groupby('id').agg({lambda x: ','.join(x)})

    df7['cast'] = s1

    df8 = df7.copy()

    #################################################

    log("QUESTION 8", output_df=df8, other=df8["cast"].head(10).values)
    return df8


def question_9(df8):
    """
    :param df9: the dataframe created in question 8
    :return: movies
            Data Type: List of strings (movie titles)
            Please read the assignment specs to know how to create the output
    """

    #################################################

    list = []

    s1 = df8['cast'].str.extractall(r"([0-9a-zA-Z]+(,[0-9a-zA-Z]+)*)")

    s1 =  df8['cast'].str.split(',').reset_index(name="cast")

    s1['new'] = s1['cast'].str.len()

    s1 = s1.sort_values(by = 'new', axis = 0, ascending=False)

    s2 = pd.merge(s1, df8, how='inner', on='id')

    i = 0

    list = []

    while i < 10:
          list.append(s2['title'][i])
          i += 1

    movies = list

    #################################################

    log("QUESTION 9", output_df=None, other=movies)
    return movies


def question_10(df8):
    """
    :param df8: the dataframe created in question 8
    :return: df10
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################

    df_copy = df8.copy()

    df_copy['release_date'] = pd.to_datetime(df_copy.release_date)

    df90 = df_copy.sort_values('release_date', ascending = False)

    df10 = df90.copy()
    
    #################################################

    log("QUESTION 10", output_df=df10, other=df10["release_date"].head(5).to_string().replace("\n", " "))
    return df10


def question_11(df10):
    """
    :param df10: the dataframe created in question 10
    :return: nothing, but saves the figure on the disk
    """

    #################################################

    s1 = df7['genres'].str.extractall(r"('name': (['])(?:(?=(\\?)).)*?)\2")

    s1 = s1[0].apply(lambda x: x.replace("'name':", ''))

    s1 = s1.apply(lambda x: x.replace("'", '')).reset_index(name="Type")

    s1 = s1.groupby('Type').size().reset_index(name="Genres")	

    unival = s1['Genres']

    unival.plot.pie(subplots=True, autopct='%.1f%%', labels=None, radius=0.9, figsize=(10, 10), pctdistance=0.9)

    plt.legend(s1['Type'], bbox_to_anchor=(0.9,1))

    #################################################

    plt.savefig("{}-Q11.png".format(studentid))


def question_12(df10):
    """
    :param df10: the dataframe created in question 10
    :return: nothing, but saves the figure on the disk
    """

    #################################################

    s1 = df10['production_countries'].str.extractall(r"('name': (['\"])(?:(?=(\\?)).)*?\2)")

    s1 = s1[0].apply(lambda x: x.replace("'name':", ''))

    s1 = s1.apply(lambda x: x.replace("'", '')).reset_index(name='Countries')

    s1 = s1.groupby('Countries').size().reset_index(name='Number of movies')

    s1 = s1.groupby('Countries').mean()

    s1 = s1.sort_values(by = 'Countries', axis = 0)

    som = s1.plot(kind='bar', figsize=(16,10))

    for i, label in enumerate(list(s1.index)):
        som.annotate(str(s1.loc[label]['Number of movies']), (i, s1.loc[label]['Number of movies']), ha='center', va='bottom')

    plt.subplots_adjust(bottom=0.4)

    #################################################

    plt.savefig("{}-Q12.png".format(studentid))


def question_13(df10):
    """
    :param df10: the dataframe created in question 10
    :return: nothing, but saves the figure on the disk
    """

    #################################################

    s1 = df10.copy()
    
    ax = s1.plot.scatter(x='vote_average', y='success_impact')
    
    for i in s1['original_language'].unique():
        sentosa_df = s1.query('original_language == "'+i+'"')
        ax = sentosa_df.plot.scatter(x='vote_average', y='success_impact', label=i, c = np.random.rand(3,), ax=ax)

    #################################################

    plt.savefig("{}-Q13.png".format(studentid))


if __name__ == "__main__":
    df1 = question_1("movies.csv", "credits.csv")
    df2 = question_2(df1)
    df3 = question_3(df2)
    df4 = question_4(df3)
    df5 = question_5(df4)
    df6 = question_6(df5)
    df7 = question_7(df6)
    df8 = question_8(df7)
    movies = question_9(df8)
    df10 = question_10(df8)
    question_11(df10)
    question_12(df10)
    question_13(df10)

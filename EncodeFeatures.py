class EncodeFeatures:
"""
requires swifter for this to work. Just copy and remove swifter on lines 74 and 94 if you like to avoid using swifter.
"""
  def __init__(self):
    """
    initializes the variables required for encoding a dataframe
    """
    self.map_history = dict()
    self.decoder_map = dict()
    self.columns_list = []

  def fit_transform(self,df, columns = None):
      """
      Applies encoding to each of the columns
      :param df: dataframe on which the encoding a column needs to be done.
      :param columns: list of one or more columns in the dataframe on which the encoding needs to be applied. If None, all the columns with dtype object will be selected.
      :return: dataframe with encoded columns
      """
      if type(df) != pd.DataFrame:
          raise ValueError("Expected {} type for param df got {} instead".format(pd.DataFrame,type(self.df)))
      else:
        self.df = df.copy()
      if not columns:
        columns = [col for col in self.df.columns if self.df[col].dtype == object]
      if type(columns) != list:
          raise ValueError("Expected {} type for param columns got {} instead".format(list,type(columns)))
      for column in columns:
        if column in self.df.columns:
          if self.map_history.get(column+'_map_'):
              continue
          else:
              self.columns_list.append(column)
              map_fun = dict(zip(self.df[column].unique(),range(len(self.df[column].unique()))))
              self.map_history[column+'_map_'] = map_fun
          self.df[column] = self.df[column].map(map_fun)
        else:
          raise KeyError('{} Column not in the dataframe'.format(column))
      return self.df

  def fit(self,df,columns=None):
    """
    creates an encoder in map_history for values in each of the columns
    :param df: dataframe on which the encoding a column needs to be done.
    :param columns: list of one or more columns in the dataframe on which the encoding needs to be applied. If None, all the columns with dtype object will be selected.
    :return: None
    """
    if type(df) != pd.DataFrame:
        raise ValueError("Expected {} type for param df got {} instead".format(pd.DataFrame,type(self.df)))
    else:
      self.df = df.copy()
    if not columns:
      columns = [col for col in self.df.columns if self.df[col].dtype == object]
    if type(columns) != list:
        raise ValueError("Expected {} type for param columns got {} instead".format(list,type(columns)))
    for column in columns:
      if column in self.df.columns:
            self.columns_list.append(column)
            map_fun = dict(zip(self.df[column].unique(),range(len(self.df[column].unique()))))
            self.map_history[column+'_map_'] = map_fun
      else:
        raise KeyError('{} Column not in the dataframe'.format(column))

  def transform(self,df=pd.DataFrame()):
    """
    transforms the df in param or the df used to initialize the object.
    :param df: dataframe if a new dataframe needs to be encoded. If None, the same df used for fitting will be transformed and returned
    :return: a dataframe with transformed columns
    """
    if df.empty:
      df = self.df.copy()
    if type(df) != pd.DataFrame:
        raise ValueError("Expected {} type for param df got {} instead".format(pd.DataFrame,type(self.df)))
    self.df = df.swifter.apply(lambda x: x.map(self.map_history[x.to_frame().columns[0] + '_map_']) if x.to_frame().columns[0] in self.columns_list else x)
    return self.df
  
  def get_decoder_map(self):
    """
    crates a decoder dictionary from encoder_map
    :return: a dictionary
    """
    return {k: {b:a for a,b in v.items()} for k,v in self.map_history.items()}

  def inverse_transform(self,df=pd.DataFrame()):
    """
    :param df: dataframe if a new dataframe needs to be decoded. If None, the same df used for fitting will be inverse transformed and returned
    :return: a dataframe with inverse transformed columns
    """
    self.decoder_map = self.get_decoder_map()
    if df.empty:
      df = self.df.copy()
    if type(df) != pd.DataFrame:
        raise ValueError("Expected {} type for param df got {} instead".format(pd.DataFrame,type(self.df)))
    return df.swifter.apply(lambda x: x.map(self.decoder_map[x.to_frame().columns[0] + '_map_']) if x.to_frame().columns[0] in self.columns_list else x)

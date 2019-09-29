import tensorflow as tf
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("sqlite:///data/divy.db")
df = pd.read_sql('availability', engine).drop(['date', 'time', 'index'], axis=1)
target = df.pop('action')

ds = tf.data.Dataset.from_tensor_slices((df.values, target.values))

train_ds = ds.shuffle(len(df)).batch(1)

model = tf.keras.Sequential([
    tf.keras.layers.Dense(10, activation='relu'),
    tf.keras.layers.Dense(10, activation='relu'),
    tf.keras.layers.Dense(1, activation='sigmoid')
])

model.compile(optimizer='adam',
              loss='mse',
              metrics=['accuracy'])

model.fit(train_ds, epochs=10)

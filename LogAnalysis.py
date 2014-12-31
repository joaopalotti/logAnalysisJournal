# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import numpy as np
import pandas as pd
import pickle
import random

from createFeatureVector import userClass

from sklearn.ensemble import ExtraTreesClassifier
from sklearn.cross_validation import cross_val_score
from sklearn.dummy import DummyClassifier
#from scipy.stats.stats import pearsonr


# Load datasets
with open("honFV.pk","rb") as f:
    hon = pickle.load(f)
with open("tripFV","rb") as f:
    trip = pickle.load(f)
#with open("aolFV","rb") as f:
#    aol = pickle.load(f)
#with open("goldMinerFV","rb") as f:
#    goldminer = pickle.load(f)

# Transform dataset into pandas objects
honValues = []
for a in hon.values():
    honValues.append(a.toDict(a.numberOfQueries-1, ["g1","g2","g4","g5","g6","g7"]))
                                                    
tripValues = []
for a in trip.values():
    tripValues.append(a.toDict(a.numberOfQueries-1, ["g1","g2","g4","g5","g6","g7"]))
    
hondf = pd.DataFrame(honValues)
tripdf = pd.DataFrame(tripValues)

hondf.to_csv("hon.csv")
tripdf.to_csv("trip.csv")

# Check some basic statistiscs
havgqps = hondf["06.AvgQueriesPerSession"]
tavgqps = tripdf["06.AvgQueriesPerSession"]
print tavgqps.mean(), tavgqps.std(), havgqps.mean(), havgqps.std()


# Using all the dataset
X = hondf.copy()
X = X.append(tripdf)

X = X.values
y = np.hstack((np.ones(hondf.shape[0]), np.zeros(tripdf.shape[0])))

etc = cross_val_score(ExtraTreesClassifier(), X, y, cv=10)
dummy = cross_val_score(DummyClassifier(), X, y, cv=10)

print "Using everything: Dummy - %f, ETC - %f " % (dummy.mean(), etc.mean())
                

# <codecell>

nusers = 1000

#Uncomment to have only users obeying some restriction
#h3 = hondf[np.modf(hondf["06.AvgQueriesPerSession"])[1] == 3]
#t3 = tripdf[np.modf(tripdf["06.AvgQueriesPerSession"])[1] == 3]

a = random.sample(hondf.index, nusers)
X = hondf.ix[a].copy()

b = random.sample(tripdf.index, nusers)
X = X.append(tripdf.ix[b].copy())

X = X.values
y = np.hstack((np.ones(nusers), np.zeros(nusers)))

dummy = cross_val_score(DummyClassifier(), X, y, cv=10)
etc = cross_val_score(ExtraTreesClassifier(), X, y, cv=10)

print "Using only %d users: Dummy - %f, ETC - %f " % (2*nusers, dummy.mean(), etc.mean())


#Some permutation, so I can have emotions
r = np.random.permutation(2000)
etr = ExtraTreesClassifier()
pred = etr.fit(X[r[1000:]],y[r[1000:]]).predict(X[:1000])



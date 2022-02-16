import numpy as np
import matplotlib.pyplot as plt
import csv

center1 = (4000, 4000)
center2 = (5000, 5000)
center3 = (6000, 6000)

distance = 4000

x1 = np.random.uniform(center1[0], center1[0] + distance, size=(420000,))
y1 = np.random.uniform(center1[1], center1[1] + distance, size=(420000,))

x2 = np.random.uniform(center2[0], center2[0] + distance, size=(420000,))
y2 = np.random.uniform(center2[1], center2[1] + distance, size=(420000,))

x3 = np.random.uniform(center3[0], center3[0] + distance, size=(420000,))
y3 = np.random.uniform(center3[1], center3[1] + distance, size=(420000,))

output = ((x1, y1), (x2, y2), (x3, y3))
with open('data-clusters.csv', 'w', newline='') as csvfile:
    for cluster in output:
        writer = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for x, y in zip(cluster[0], cluster[1]):
            writer.writerow([x,y])

def average(numbers):
	if not numbers :
		return -1
	import numpy as np
	return round(np.average(numbers), 1)


def median(numbers):
	if not numbers :
		return -1
	import numpy as np
	return round(np.median(numbers), 1)

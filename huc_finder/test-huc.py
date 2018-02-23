from huc import HucFinder

if __name__ == '__main__':
	hucfinder = HucFinder('huc-all.shp')
	print(hucfinder.getHuc(lat=38, lon=-85.74))
	print(hucfinder.getHuc(lat=39, lon=-85.74))
	print(hucfinder.getHuc(lat=99, lon=-85.74))
	print(hucfinder.getHuc(lat=37, lon=-84.00))
	print(hucfinder.getHuc(lat=40, lon=-84.55))
	print(hucfinder.getHuc(lat=36, lon=-82.74))
	print(hucfinder.getHuc(lat=38, lon=-83.74))
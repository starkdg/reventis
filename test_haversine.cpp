#include <iostream>
#include <iomanip>
#include <cmath>
#include <limits>
#include <cassert>
#include "haversine.hpp"

using namespace std;

void test_haversine(){

	Position a1;   // Hartford
	a1.lon = -72.676311; 
	a1.lat = 41.768115;
	

	Position a2;   // Boston 
	a2.lon = -71.020416;
	a2.lat = 42.358930;

	// 151.659746
	cout << setprecision(numeric_limits<long double>::digits10 + 1);
	double dist1 = haversine_distance(a1, a2);
	double dist2 = haversine_distance(a2, a1);
	cout << "distance(Hartford, Boston) = " << dist1 << " km" << endl;
	cout << "distance(Boston, Hartford) = " << dist2 << " km" << endl;
	assert(fabs(dist1 - dist2) <= 1e-6);
	assert(fabs(dist1 - 151.659746) <= 1e-6);

	Position b1; // NYC
	b1.lon = -73.983360;
	b1.lat = 40.727048;
	
	Position b2; // London
	b2.lon = -0.094226;
	b2.lat = 51.505058;

	// 5570.030078 km
	dist1 = haversine_distance(b1, b2);
	dist2 = haversine_distance(b2, b1);
	cout << "distance(NYC, London) = " << dist1 << " km" << endl;
	cout << "distance(London, NYC) = " << dist2 << " km" << endl;
	assert(fabs(dist1 - dist2) <= 1e-6);
	assert(fabs(dist1 - 5570.030078) <= 1e-6);
	

	Position c1; // Miami
	c1.lon = -80.227401;
	c1.lat = 25.762913;

	Position c2; // Bakersfield CA
	c2.lon = -119.075686;
	c2.lat = 35.323697;

	// 3842.368669 km
	dist1 = haversine_distance(c1, c2);
	dist2 = haversine_distance(c2, c1);
	cout << "distance(Miami, BakersfieldCA) = " << dist1 << " km" << endl;
	cout << "distance(BakersfieldCA, Miami) = " << dist2 << " km" << endl;
	assert(fabs(dist1 - dist2) <= 1e-06);
	assert(fabs(dist1 - 3842.368669) <= 1e-06);
	
	Position d1; // Mexico City
	d1.lon = -99.086630;
	d1.lat = 19.386793;

	Position d2; // Bogota, Columbia
	d2.lon = -74.110563;
	d2.lat = 4.581895;

	// 3168.347571
	dist1 = haversine_distance(d1, d2);
	dist2 = haversine_distance(d2, d1);
	cout << "distance(Mexico, Columbia) = " << dist1 << " km" << endl;
	cout << "distance(Columbia, Mexico) = " << dist2 << " km" << endl;
	assert(fabs(dist1 - dist2) <= 1e-06);
	assert(fabs(dist1 - 3168.347571) <= 1e-06);
}

void test_get_xyradius(){

	Position center; // Cragin
	center.lon = -72.333344;
	center.lat = 41.574519;

	const double radius = 10; // 10 km
	double xdelta, ydelta;
	get_xyradius(center, radius, xdelta, ydelta);


	Position a;
	a.lon = center.lon - xdelta;
	a.lat = center.lat;

	Position b;
	b.lon = center.lon + xdelta;
	b.lat = center.lat;

	cout << "(" << a.lon << "," << a.lat << ") to (" << b.lon << "," << b.lat << ")" << endl;
	double d1 = haversine_distance(a, b);
	double d2 = haversine_distance(b, a);
	cout << "distance = " << d1 << endl;
	cout << "distance = " << d2 << endl;
	cout << "error = " << fabs(d1 - 20.0) << endl;
	assert(fabs(d1 - d2) <= 0.0001);
	assert(fabs(d1 - 20.000000) <= 0.0001);


	a.lon = center.lon - xdelta;
	a.lat = center.lat - ydelta;
	b.lon = center.lon + xdelta;
	b.lat = center.lat + ydelta;

	cout << "(" << a.lon << "," << a.lat << ") to (" << b.lon << "," << b.lat << ")" << endl;
	d1 = haversine_distance(a, b);
	d2 = haversine_distance(b, a);
	cout << "distance = " << d1 << endl;
	cout << "distance = " << d2 << endl;
	assert(fabs(d1 - d2) <= 0.0001);
	assert(fabs(d1 - 28.284248) <= 0.0001);
}


int main(int argc, char **argv){


	cout << "Test Haversine distance function" << endl;
	test_haversine();

	cout << endl << "Test Get XY Radius" << endl;
	test_get_xyradius();


	return 0;
}

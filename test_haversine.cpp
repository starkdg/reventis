#include <iostream>
#include <iomanip>
#include <cmath>
#include <limits>
#include <cassert>
#include "haversine.hpp"

using namespace std;

void test_haversine(){

	Position a1;   // Hartford
	a1.x = -72.676311; 
	a1.y = 41.768115;
	

	Position a2;   // Boston 
	a2.x = -71.020416;
	a2.y = 42.358930;

	// 151.659746
	cout << setprecision(numeric_limits<long double>::digits10 + 1);
	double dist1 = haversine_distance(a1, a2);
	double dist2 = haversine_distance(a2, a1);
	cout << "distance(Hartford, Boston) = " << dist1 << " km" << endl;
	cout << "distance(Boston, Hartford) = " << dist2 << " km" << endl;
	assert(fabs(dist1 - dist2) <= 1e-6);
	assert(fabs(dist1 - 151.659746) <= 1e-6);

	Position b1; // NYC
	b1.x = -73.983360;
	b1.y = 40.727048;
	
	Position b2; // London
	b2.x = -0.094226;
	b2.y = 51.505058;

	// 5570.030078 km
	dist1 = haversine_distance(b1, b2);
	dist2 = haversine_distance(b2, b1);
	cout << "distance(NYC, London) = " << dist1 << " km" << endl;
	cout << "distance(London, NYC) = " << dist2 << " km" << endl;
	assert(fabs(dist1 - dist2) <= 1e-6);
	assert(fabs(dist1 - 5570.030078) <= 1e-6);
	

	Position c1; // Miami
	c1.x = -80.227401;
	c1.y = 25.762913;

	Position c2; // Bakersfield CA
	c2.x = -119.075686;
	c2.y = 35.323697;

	// 3842.368669 km
	dist1 = haversine_distance(c1, c2);
	dist2 = haversine_distance(c2, c1);
	cout << "distance(Miami, BakersfieldCA) = " << dist1 << " km" << endl;
	cout << "distance(BakersfieldCA, Miami) = " << dist2 << " km" << endl;
	assert(fabs(dist1 - dist2) <= 1e-06);
	assert(fabs(dist1 - 3842.368669) <= 1e-06);
	
	Position d1; // Mexico City
	d1.x = -99.086630;
	d1.y = 19.386793;

	Position d2; // Bogota, Columbia
	d2.x = -74.110563;
	d2.y = 4.581895;

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
	center.x = -72.333344;
	center.y = 41.574519;

	const double radius = 10; // 10 km
	double xdelta, ydelta;
	get_xyradius(center, radius, xdelta, ydelta);


	Position a;
	a.x = center.x - xdelta;
	a.y = center.y;

	Position b;
	b.x = center.x + xdelta;
	b.y = center.y;

	cout << "(" << a.x << "," << a.y << ") to (" << b.x << "," << b.y << ")" << endl;
	double d1 = haversine_distance(a, b);
	double d2 = haversine_distance(b, a);
	cout << "distance = " << d1 << endl;
	cout << "distance = " << d2 << endl;
	cout << "error = " << fabs(d1 - 20.0) << endl;
	assert(fabs(d1 - d2) <= 0.0001);
	assert(fabs(d1 - 20.000000) <= 0.0001);


	a.x = center.x - xdelta;
	a.y = center.y - ydelta;
	b.x = center.x + xdelta;
	b.y = center.y + ydelta;

	cout << "(" << a.x << "," << a.y << ") to (" << b.x << "," << b.y << ")" << endl;
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

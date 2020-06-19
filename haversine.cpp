#include <iostream>
#include <iomanip>
#include <cmath>
#include <limits>
#include <cassert>
#include "haversine.hpp"

using namespace std;

static const double m_pi = 3.1415926535897932384626433832795028841971693;

static const double m_earth_radius = 6371.0; // earth radius (km)

double haversine_distance(const Position &a, const Position &b){
	double longitude_delta = (b.x - a.x)*m_pi/180.0;
	double latitude_delta = (b.y - a.y)*m_pi/180.0;
	double latitude_hav = sin(latitude_delta/2.0);
	latitude_hav *= latitude_hav;
	double longitude_hav = sin(longitude_delta/2.0);
	longitude_hav *= longitude_hav;
	double latitude_cos = cos(a.y*m_pi/180.0)*cos(b.y*m_pi/180.0);
	double tmp = latitude_hav + latitude_cos*longitude_hav;
	return  2*m_earth_radius*asin(sqrt(tmp));
}


void get_xyradius(const Position &center, const double radius, double &delta_x, double &delta_y){
	double tmp =  sin(radius/(2*m_earth_radius))/cos(center.y*m_pi/180.0);
	delta_x = 2*asin(tmp)*180.0/m_pi;
	delta_y = (radius/m_earth_radius)*180.0/m_pi;
}



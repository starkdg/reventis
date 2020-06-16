#ifndef _HAVERSINE_H
#define _HAVERSINE_H

typedef struct pos_t {
	double lon; 
	double lat;
} Position;

double haversine_distance(const Position &a, const Position &b);

void get_xyradius(const Position &center, const double radius, double &delta_x, double &delta_y);


#endif /* _HAVERSINE_H */ 

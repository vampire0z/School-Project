package A2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Point3D implements Serializable {
	public double x, y, z;
	public Point3D() {}
	
	public Point3D(double x, double y, double z) {
		this.x = x;
		this.y = y;
		this.z = z;
	}
	public Point3D add(Point3D other) {
		x += other.x;
		y += other.y;
		z += other.z;
		return this;
	}
	public Point3D div(long val) {
		x /= val;
		y /= val;
		z /= val;
		return this;
	}
	public double euclideanDistance(Point3D other) {
		return Math.sqrt((x-other.x)*(x-other.x) + (y-other.y)*(y-other.y) + (z-other.z)*(z-other.z));
	}
	public void clear() {
		x = y = z =  0.0;
	}
	public String toString() {
		return x + " " + y + " " + z;
	}
}
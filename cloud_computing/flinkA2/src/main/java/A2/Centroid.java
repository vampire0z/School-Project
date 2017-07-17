package A2;

/**
 * A simple 3-dimensional centroid, basically a point with an ID.
 */
public class Centroid extends Point3D {
	public int id;
	public Centroid() {}
	public Centroid(int id, double x, double y, double z) {
		super(x,y,z);
		this.id = id;
	}
	public Centroid(int id, Point3D p) {
		super(p.x, p.y, p.z);
		this.id = id;
	}
	public String toString() {
		return id + " " + super.toString();
	}
}
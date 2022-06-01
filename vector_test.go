//go:build go1.18
// +build go1.18

package godal

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestClosingErrors(t *testing.T) {
	//hacky test to force Dataset.Close() to return an error. we use the fact that
	//the geojson drivers uses a temp file when updating an exisiting dataset

	tmpdir, _ := ioutil.TempDir("", "")
	fname := filepath.Join(tmpdir, "tt.json")
	defer func() {
		_ = os.Chmod(fname, 0777)
		_ = os.Chmod(tmpdir, 0777)
		_ = os.RemoveAll(tmpdir)
	}()
	sds, err := Open("testdata/test.geojson")
	assert.NoError(t, err)
	rds, err := sds.VectorTranslate(fname, []string{"-f", "GeoJSON"})
	assert.NoError(t, err)
	_ = sds.Close()
	_ = rds.Close()
	rds, _ = Open(fname, Update())
	_ = os.Chmod(fname, 0400)
	_ = os.Chmod(tmpdir, 0400)
	_ = rds.SetMetadata("foo", "bar")
	rds.Layers()[0].ResetReading()
	f := rds.Layers()[0].NextFeature()
	ng, err := f.Geometry().Buffer(1, 1)
	assert.NoError(t, err)
	_ = f.SetGeometry(ng)
	_ = rds.Layers()[0].UpdateFeature(f)
	err = rds.Close()
	assert.Error(t, err)
}

func TestPolygonize(t *testing.T) {
	rds, _ := Create(Memory, "", 2, Byte, 8, 8)
	vds, err := CreateVector(Memory, "")
	if err != nil {
		t.Fatal(err)
	}
	pl4, _ := vds.CreateLayer("p4", nil, GTPolygon)
	pl8, _ := vds.CreateLayer("p8", nil, GTPolygon)
	data := make([]byte, 64)
	for r := 0; r < 8; r++ {
		for c := 0; c < 8; c++ {
			if r == c {
				data[r*8+c] = 128
			} else {
				data[r*8+c] = 64
			}
		}
	}
	bnd := rds.Bands()[0]
	_ = bnd.Write(0, 0, data, 8, 8)
	err = bnd.Polygonize(pl4, PixelValueFieldIndex(5))
	assert.Error(t, err, "invalid field not raised")
	ehc := eh()
	err = bnd.Polygonize(pl4, PixelValueFieldIndex(5), ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err, "invalid field not raised")

	err = bnd.Polygonize(pl4)
	if err != nil {
		t.Error(err)
	}
	cnt, _ := pl4.FeatureCount()
	if cnt != 10 {
		t.Errorf("got %d/10 polys", cnt)
	}
	err = bnd.Polygonize(pl8, EightConnected())
	if err != nil {
		t.Error(err)
	}
	cnt, _ = pl8.FeatureCount()
	if cnt != 2 {
		t.Errorf("got %d/2 polys", cnt)
	}

	msk, err := bnd.CreateMask(0x02)
	if err != nil {
		t.Fatal(err)
	}
	for r := 0; r < 8; r++ {
		for c := 0; c < 8; c++ {
			if r == c {
				data[r*8+c] = 0
			} else {
				data[r*8+c] = 255
			}
		}
	}
	_ = msk.Write(0, 0, data, 8, 8)

	nd, _ := vds.CreateLayer("nd", nil, GTPolygon, NewFieldDefinition("unused", FTString), NewFieldDefinition("c", FTInt))
	err = bnd.Polygonize(nd, PixelValueFieldIndex(1))
	if err != nil {
		t.Error(err)
	}
	cnt, _ = nd.FeatureCount()
	if cnt != 2 {
		t.Errorf("got %d/2 polys", cnt)
	}
	feature := nd.NextFeature()
	fields := feature.Fields()
	cField, err := fields.GetByName("c")
	assert.NoError(t, err)
	fvq := NewFieldValueQuerier[int](feature)
	cVal, err := fvq.GetValue(cField)
	assert.NoError(t, err)
	if cVal != 64 && cVal != 128 {
		t.Error("expecting 64 or 128 for pixel attribute")
	}
	nm, _ := vds.CreateLayer("nm", nil, GTPolygon)
	err = bnd.Polygonize(nm, NoMask())
	if err != nil {
		t.Error(err)
	}
	cnt, _ = nm.FeatureCount()
	if cnt != 10 {
		t.Errorf("got %d/10 polys", cnt)
	}

	//one quarter is nodata
	for r := 0; r < 8; r++ {
		for c := 0; c < 8; c++ {
			if r < 4 && c < 4 {
				data[r*8+c] = 0
			} else {
				data[r*8+c] = 255
			}
		}
	}
	_ = rds.Bands()[1].Write(0, 0, data, 8, 8)
	for r := 0; r < 8; r++ {
		for c := 0; c < 8; c++ {
			data[r*8+c] = uint8(r*8 + c)
		}
	}
	_ = bnd.Write(0, 0, data, 8, 8)

	md, _ := vds.CreateLayer("md", nil, GTPolygon)
	err = bnd.Polygonize(md, Mask(rds.Bands()[1]))
	if err != nil {
		t.Error(err)
	}
	cnt, _ = md.FeatureCount()
	if cnt != 48 { // 48 == 64 - 64/4
		t.Errorf("got %d/48 polys", cnt)
	}
}

func TestRasterize(t *testing.T) {
	tf := tempfile()
	defer os.Remove(tf)
	inv, _ := Open("testdata/test.geojson", VectorOnly())

	_, err := inv.Rasterize(tf, []string{"-of", "bogus"})
	assert.Error(t, err)
	ehc := eh()
	_, err = inv.Rasterize(tf, []string{"-of", "bogus"}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	dl := &debugLogger{}
	rds, err := inv.Rasterize(tf, []string{
		"-te", "99", "-1", "102", "2",
		"-ts", "9", "9",
		"-init", "10",
		"-burn", "20"}, CreationOption("TILED=YES"), GTiff, ErrLogger(dl.L), ConfigOption("CPL_DEBUG=ON"))
	assert.NoError(t, err)
	assert.NotEmpty(t, dl.logs)
	defer rds.Close()
	data := make([]byte, 81)
	err = rds.Read(0, 0, data, 9, 9)
	if err != nil {
		t.Fatal(err)
	}
	n10 := 0
	n20 := 0
	for i := range data {
		if data[i] == 10 {
			n10++
		}
		if data[i] == 20 {
			n20++
		}
	}
	if n10 != 72 || n20 != 9 {
		t.Errorf("10/20: %d/%d expected 72/9", n10, n20) //not really tested here, although sum should always be 81
	}

}

func TestRasterizeGeometries(t *testing.T) {
	vds, _ := Open("testdata/test.geojson")
	//ext is 100,0,101,1
	defer vds.Close()
	mds, _ := Create(Memory, "", 3, Byte, 300, 300)
	defer mds.Close()
	_ = mds.SetGeoTransform([6]float64{99, 0.01, 0, 2, 0, -0.01}) //set extent to 99,-1,102,2
	bnds := mds.Bands()

	ff := vds.Layers()[0].NextFeature().Geometry()

	for _, bnd := range bnds {
		_ = bnd.Fill(255, 0)
	}
	data := make([]byte, 300) //to extract a 10x10 window

	err := mds.RasterizeGeometry(ff)
	assert.NoError(t, err)
	_ = mds.Read(95, 95, data, 10, 10)
	assert.Equal(t, []byte{255, 255, 255}, data[0:3])
	assert.Equal(t, []byte{0, 0, 0}, data[297:300])

	alldata1 := make([]byte, 300*300*3)
	_ = mds.Read(0, 0, alldata1, 300, 300)
	alldata2 := make([]byte, 300*300*3)
	err = mds.RasterizeGeometry(ff, AllTouched())
	assert.NoError(t, err)
	_ = mds.Read(0, 0, alldata2, 300, 300)
	assert.NotEqual(t, alldata1, alldata2)

	err = mds.RasterizeGeometry(ff, Values(200))
	assert.NoError(t, err)
	_ = mds.Read(95, 95, data, 10, 10)
	assert.Equal(t, []byte{200, 200, 200}, data[297:300])

	err = mds.RasterizeGeometry(ff, Bands(0), Values(100))
	assert.NoError(t, err)
	_ = mds.Read(95, 95, data, 10, 10)
	assert.Equal(t, []byte{100, 200, 200}, data[297:300])

	err = mds.RasterizeGeometry(ff, Values(1, 2, 3))
	assert.NoError(t, err)
	_ = mds.Read(95, 95, data, 10, 10)
	assert.Equal(t, []uint8{1, 2, 3}, data[297:300])

	err = mds.RasterizeGeometry(ff, Bands(0, 1), Values(5, 6))
	assert.NoError(t, err)
	_ = mds.Read(95, 95, data, 10, 10)
	assert.Equal(t, []uint8{5, 6, 3}, data[297:300])

	err = mds.RasterizeGeometry(ff, Bands(0), Values(1, 2))
	assert.Error(t, err)
	err = mds.RasterizeGeometry(ff, Bands(0, 2, 3), Values(1, 2, 3))
	assert.Error(t, err)
	ehc := eh()
	err = mds.RasterizeGeometry(ff, Bands(0, 2, 3), Values(1, 2, 3), ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

}

func TestVectorTranslate(t *testing.T) {
	tmpname := tempfile()
	defer os.Remove(tmpname)
	ds, err := Open("testdata/test.geojson", VectorOnly())
	assert.NoError(t, err)

	st1, _ := os.Stat("testdata/test.geojson")
	nds, err := ds.VectorTranslate(tmpname, []string{"-lco", "RFC7946=YES"}, GeoJSON)
	assert.NoError(t, err)

	_ = nds.SetMetadata("baz", "boo")
	err = nds.Close()
	assert.NoError(t, err)

	st2, _ := os.Stat(tmpname)
	if st2.Size() == 0 || st1.Size() == st2.Size() {
		t.Error("invalid size")
	}

	err = RegisterVector("TAB")
	assert.NoError(t, err)

	tmpdir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(tmpdir)
	dl := &debugLogger{}
	mds, err := ds.VectorTranslate(filepath.Join(tmpdir, "test.mif"), []string{"-f", "Mapinfo File"}, CreationOption("FORMAT=MIF"),
		ErrLogger(dl.L), ConfigOption("CPL_DEBUG=ON"))
	assert.NoError(t, err)
	assert.NotEmpty(t, dl.logs)
	mds.Close()

	_, err = ds.VectorTranslate("foobar", []string{"-f", "bogusdriver"})
	assert.Error(t, err)
	ehc := eh()
	_, err = ds.VectorTranslate("foobar", []string{"-f", "bogusdriver"}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
}

func TestVectorLayer(t *testing.T) {
	rds, _ := Create(Memory, "", 3, Byte, 10, 10)
	_, err := rds.CreateLayer("ff", nil, GTPolygon)
	assert.Error(t, err)
	ehc := eh()
	_, err = rds.CreateLayer("ff", nil, GTPolygon, ErrLogger(ehc.ErrorHandler))

	assert.Error(t, err)
	lyrs := rds.Layers()
	if len(lyrs) > 0 {
		t.Error("raster ds has vector layers")
	}
	rds.Close()
	tmpname := tempfile()
	defer os.Remove(tmpname)
	ds, err := Open("testdata/test.geojson", VectorOnly())
	if err != nil {
		t.Fatal(err)
	}
	assert.Nil(t, ds.Bands())
	assert.Error(t, ds.BuildOverviews())
	assert.Error(t, ds.ClearOverviews())
	ehc = eh()
	assert.Error(t, ds.ClearOverviews(ErrLogger(ehc.ErrorHandler)))
	assert.Error(t, ds.SetNoData(0))
	ehc = eh()
	assert.Error(t, ds.SetNoData(0, ErrLogger(ehc.ErrorHandler)))
	buf := make([]byte, 10)
	ehc = eh()
	assert.Error(t, ds.Read(0, 0, buf, 3, 3))
	assert.Error(t, ds.Read(0, 0, buf, 3, 3, ErrLogger(ehc.ErrorHandler)))
	ehc = eh()
	assert.Error(t, ds.Write(0, 0, buf, 3, 3))
	assert.Error(t, ds.Write(0, 0, buf, 3, 3, ErrLogger(ehc.ErrorHandler)))

	sr3857, _ := NewSpatialRefFromEPSG(3857)
	defer sr3857.Close()

	layer := ds.Layers()[0]
	assert.Equal(t, layer.Name(), "test")
	assert.Equal(t, layer.Type(), GTPolygon)
	bounds, err := layer.Bounds()
	assert.NoError(t, err)
	assert.Equal(t, bounds, [4]float64{100, 0, 101, 1})
	_, err = layer.Bounds(sr3857)
	assert.NoError(t, err)
	_, err = layer.Bounds(&SpatialRef{})
	assert.Error(t, err)
	_, err = Layer{}.Bounds(ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	assert.Nil(t, ds.LayerByName("none"))
	testLayer := ds.LayerByName("test")
	assert.NotNil(t, testLayer)
	vds, _ := CreateVector(Memory, "")
	copiedLayer, err := vds.CopyLayer(*testLayer, "copied")
	assert.NoError(t, err)
	testLayer.ResetReading()
	feature := testLayer.NextFeature()
	err = copiedLayer.CreateFeature(feature)
	assert.NoError(t, err)
	err = copiedLayer.CreateFeature(&Feature{}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	_, err = vds.CopyLayer(Layer{}, "empty", ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	_ = vds.Close()

	dds, err := ds.VectorTranslate("", []string{"-of", "MEMORY"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = (&Geometry{}).Buffer(10, 1)
	assert.Error(t, err)
	ehc = eh()
	_, err = (&Geometry{}).Buffer(10, 1, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	_, err = (&Geometry{}).Simplify(1)
	assert.Error(t, err)
	ehc = eh()
	_, err = (&Geometry{}).Simplify(1, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	err = (&Feature{}).SetGeometry(&Geometry{})
	assert.Error(t, err)
	ehc = eh()
	err = (&Feature{}).SetGeometry(&Geometry{}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	sr4326, _ := NewSpatialRefFromEPSG(4326)
	defer sr4326.Close()
	l2, err := dds.CreateLayer("t2", sr4326, GTPoint)
	assert.NoError(t, err)
	assert.True(t, sr4326.IsSame(l2.SpatialRef()))
	l := dds.Layers()[0]
	l.ResetReading()
	_, err = l.FeatureCount()
	assert.NoError(t, err)
	_, err = Layer{}.FeatureCount()
	assert.Error(t, err)
	ehc = eh()
	cnt, err := l.FeatureCount(ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	ehc = eh()
	_, err = Layer{}.FeatureCount(ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	i := 0
	for {
		ff := l.NextFeature()
		if ff == nil {
			break
		}
		i++
		og := ff.Geometry()
		if i == 1 {
			bounds, _ := og.Bounds()
			assert.Equal(t, [4]float64{100, 0, 101, 1}, bounds)
			b3857, err := og.Bounds(sr3857)
			assert.NoError(t, err)
			assert.NotEqual(t, bounds, b3857)
		}
		bg, err := og.Buffer(0.01, 1)
		assert.NoError(t, err)
		og.Close()
		sg, err := bg.Simplify(0.01)
		assert.NoError(t, err)
		bg.Close()
		assert.NotPanics(t, bg.Close, "2nd geom close must not panic")
		err = ff.SetGeometry(sg)
		assert.NoError(t, err)

		em, err := sg.Buffer(-200, 1)
		assert.NoError(t, err)
		if !em.Empty() {
			t.Error("-200 buf not empty")
		}

		em.Close()
		sg.Close()
		err = l.UpdateFeature(ff)
		assert.NoError(t, err)
		ehc = eh()
		err = l.UpdateFeature(ff, ErrLogger(ehc.ErrorHandler))
		assert.NoError(t, err)
		ff.Close()
		assert.NotPanics(t, ff.Close, "second close must not panic")
	}
	if i != 2 || i != cnt {
		t.Error("wrong feature count")
	}
	err = dds.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestLayerModifyFeatures(t *testing.T) {
	ds, _ := Open("testdata/test.geojson") //read-only
	defer ds.Close()
	l := ds.Layers()[0]
	for {
		ff := l.NextFeature()
		if ff == nil {
			break
		}
		err := l.DeleteFeature(ff)
		assert.Error(t, err) //read-only, must fail
		ehc := eh()
		err = l.DeleteFeature(ff, ErrLogger(ehc.ErrorHandler))
		assert.Error(t, err) //read-only, must fail
		err = l.UpdateFeature(ff)
		assert.Error(t, err) //read-only, must fail
		ehc = eh()
		err = l.UpdateFeature(ff, ErrLogger(ehc.ErrorHandler))
		assert.Error(t, err) //read-only, must fail
	}
	dsm, _ := ds.VectorTranslate("", []string{"-of", "Memory"})
	defer dsm.Close()
	l = dsm.Layers()[0]
	for {
		ff := l.NextFeature()
		if ff == nil {
			break
		}
		err := l.DeleteFeature(ff)
		assert.NoError(t, err) //read-write, must not fail
	}
	c, _ := l.FeatureCount()
	assert.Equal(t, 0, c)

}

func TestNewGeometry(t *testing.T) {
	_, err := NewGeometryFromWKT("babsaba", &SpatialRef{})
	assert.Error(t, err)
	ehc := eh()
	_, err = NewGeometryFromWKT("babsaba", &SpatialRef{}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	gp, err := NewGeometryFromWKT("POINT (30 10)", nil)
	assert.NoError(t, err)
	assert.NotNil(t, gp)
	ehc = eh()
	gp, err = NewGeometryFromWKT("POINT (30 10)", nil, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	wkt, err := gp.WKT()
	assert.NoError(t, err)
	assert.Equal(t, "POINT (30 10)", wkt)
	ehc = eh()
	_, err = gp.WKT(ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	wkb, err := gp.WKB()
	assert.NoError(t, err)
	assert.NotEmpty(t, wkb)
	ehc = eh()
	_, err = gp.WKB(ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	gp.Close()

	_, err = NewGeometryFromWKB(wkb[0:10], &SpatialRef{})
	assert.Error(t, err)
	ehc = eh()
	_, err = NewGeometryFromWKB(wkb[0:10], &SpatialRef{}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	gp, err = NewGeometryFromWKB(wkb, nil)
	assert.NoError(t, err)
	assert.NotNil(t, gp)
	ehc = eh()
	gp, err = NewGeometryFromWKB(wkb, nil, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	assert.NotNil(t, gp)

	wkt, err = gp.WKT()
	assert.NoError(t, err)
	assert.Equal(t, "POINT (30 10)", wkt)

	_, err = (&Geometry{}).WKB()
	assert.Error(t, err)

	_, err = (&Geometry{}).WKT()
	assert.Error(t, err)
}

func TestNewGeometryFromGeoJSON(t *testing.T) {
	jsonStr := `{ "type": "Polygon", "coordinates": [ [ [ -71.7, 44.9 ], [ -71.8, 45.1 ], [ -71.6, 45.2 ], [ -70.6, 45.3 ], [ -71.7, 44.9 ] ] ] }`

	_, err := NewGeometryFromGeoJSON("babsaba")
	assert.Error(t, err)
	ehc := eh()
	_, err = NewGeometryFromGeoJSON("babsaba", ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	gp, err := NewGeometryFromGeoJSON(jsonStr)
	assert.NoError(t, err)
	assert.NotNil(t, gp)
	ehc = eh()
	gp, err = NewGeometryFromGeoJSON(jsonStr, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	outJSON, err := gp.GeoJSON()
	assert.NoError(t, err)
	assert.Equal(t, jsonStr, outJSON)
}

func TestGeometryDifference(t *testing.T) {
	sr, _ := NewSpatialRefFromEPSG(4326)
	defer sr.Close()

	polyStr := "POLYGON ((0 0,2 0,2 2,0 2,0 0))"
	polyGeom1, _ := NewGeometryFromWKT(polyStr, sr)
	polyStr = "POLYGON ((0 0,1 0,1 1,0 1,0 0))"
	polyGeom2, _ := NewGeometryFromWKT(polyStr, sr)

	diffGeom, err := polyGeom1.Difference(polyGeom2)
	assert.NoError(t, err)
	assert.Equal(t, diffGeom.Area(), 3.0)

	_, err = polyGeom1.Difference(&Geometry{})
	assert.Error(t, err)

	ehc := eh()
	_, err = (&Geometry{}).Difference(polyGeom2, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
}

func TestGeometryUnion(t *testing.T) {
	sr, _ := NewSpatialRefFromEPSG(4326)
	defer sr.Close()

	polyStr := "POLYGON ((0 0,2 0,2 2,0 2,0 0))"
	polyGeom1, _ := NewGeometryFromWKT(polyStr, sr)
	polyStr = "POLYGON ((1 1,3 1,3 3,1 3,1 1))"
	polyGeom2, _ := NewGeometryFromWKT(polyStr, sr)

	diffGeom, err := polyGeom1.Union(polyGeom2)
	assert.NoError(t, err)
	assert.Equal(t, diffGeom.Area(), 7.0)

	_, err = polyGeom1.Union(&Geometry{})
	assert.Error(t, err)

	ehc := eh()
	_, err = (&Geometry{}).Union(polyGeom2, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
}

func TestGeometryIntersects(t *testing.T) {
	_, err := (&Geometry{}).Intersects(&Geometry{})
	assert.Error(t, err)

	ehc := eh()
	_, err = (&Geometry{}).Intersects(&Geometry{}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	poly1Str := `{ "type": "Polygon", "coordinates": [ [ [ 0, 0 ], [ 1, 0 ], [ 1, 1 ], [ 0, 1 ], [ 0, 0 ] ] ] }`
	poly2Str := `{ "type": "Polygon", "coordinates": [ [ [ 2, 0 ], [ 3, 0 ], [ 3, 1 ], [ 2, 1 ], [ 2, 0 ] ] ] }`

	gp1, err := NewGeometryFromGeoJSON(poly1Str)
	assert.NoError(t, err)
	assert.NotNil(t, gp1)

	gp2, err := NewGeometryFromGeoJSON(poly2Str)
	assert.NoError(t, err)
	assert.NotNil(t, gp2)

	_, err = gp1.Intersects(&Geometry{})
	assert.Error(t, err)

	_, err = (&Geometry{}).Intersects(gp1)
	assert.Error(t, err)

	ret, err := gp1.Intersects(gp1)
	assert.NoError(t, err)
	assert.True(t, ret)

	ehc = eh()
	ret, err = gp1.Intersects(gp1, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	assert.True(t, ret)

	ret, err = gp1.Intersects(gp2)
	assert.NoError(t, err)
	assert.False(t, ret)

	ehc = eh()
	ret, err = gp1.Intersects(gp2, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	assert.False(t, ret)
}

func TestGeomToGeoJSON(t *testing.T) {
	sr, _ := NewSpatialRefFromEPSG(4326)
	g, _ := NewGeometryFromWKT("POINT (10.123456789 10)", sr)
	gj, err := g.GeoJSON()
	assert.NoError(t, err)
	assert.Equal(t, `{ "type": "Point", "coordinates": [ 10.1234568, 10.0 ] }`, gj)

	gj, err = g.GeoJSON(SignificantDigits(3))
	assert.NoError(t, err)
	assert.Equal(t, `{ "type": "Point", "coordinates": [ 10.123, 10.0 ] }`, gj)

	_, err = (&Geometry{}).GeoJSON()
	assert.Error(t, err)
	ehc := eh()
	_, err = (&Geometry{}).GeoJSON(ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

}

func TestGeometryToGML(t *testing.T) {
	sr, _ := NewSpatialRefFromEPSG(4326)
	defer sr.Close()

	polyStr := "POLYGON ((0 0,2 0,2 2,0 2,0 0))"
	polyGeom, _ := NewGeometryFromWKT(polyStr, sr)

	gml, err := polyGeom.GML()
	assert.NoError(t, err)
	assert.Equal(t, gml, `<gml:Polygon srsName="EPSG:4326"><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>0,0 2,0 2,2 0,2 0,0</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>`)

	gml, err = polyGeom.GML(CreationOption("FORMAT=GML3", "SRSNAME_FORMAT=OGC_URN"))
	assert.NoError(t, err)
	assert.Equal(t, gml, `<gml:Polygon srsName="urn:ogc:def:crs:EPSG::4326"><gml:exterior><gml:LinearRing><gml:posList>0 0 0 2 2 2 2 0 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>`)

	ehc := eh()
	_, err = polyGeom.GML(CreationOption("FORMAT=GML3", "SRSNAME_FORMAT=fake"), ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
}

func TestGeometryBounds(t *testing.T) {
	sr4326, _ := NewSpatialRefFromEPSG(4326)
	sr3857, _ := NewSpatialRefFromEPSG(3857)
	box, err := NewGeometryFromWKT("POLYGON((-180 -91,-180 90,180 90,180 -90,-180 -91))", sr4326)
	assert.NoError(t, err)
	_, err = box.Bounds(sr3857)
	assert.Error(t, err)
	_, err = box.Bounds(&SpatialRef{handle: nil})
	assert.Error(t, err)

}

func TestGeometryTransform(t *testing.T) {
	sr, _ := NewSpatialRefFromEPSG(4326)
	srm, _ := NewSpatialRefFromEPSG(3857)
	gp, _ := NewGeometryFromWKT("POINT (10 10)", sr)
	assert.True(t, gp.SpatialRef().IsSame(sr))

	err := gp.Reproject(srm)
	assert.NoError(t, err)
	assert.True(t, gp.SpatialRef().IsSame(srm))
	gp.Close()

	ehc := eh()
	gp, _ = NewGeometryFromWKT("POINT (10 10)", sr)
	err = gp.Reproject(srm, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	nwkt, _ := gp.WKT()
	assert.NotEqual(t, "POINT (10 10)", nwkt)
	gp.SetSpatialRef(sr)
	assert.True(t, gp.SpatialRef().IsSame(sr))

	gp.Close()

	gp, _ = NewGeometryFromWKT("POINT (10 91)", sr)
	err = gp.Reproject(srm)
	assert.Error(t, err)
	gp.Close()

	ehc = eh()
	gp, _ = NewGeometryFromWKT("POINT (10 91)", sr, ErrLogger(ehc.ErrorHandler))
	err = gp.Reproject(srm)
	assert.Error(t, err)
	gp.Close()

	trn, _ := NewTransform(sr, srm)
	gp, _ = NewGeometryFromWKT("POINT (10 10)", nil)

	err = gp.Transform(trn)
	assert.NoError(t, err)
	assert.True(t, gp.SpatialRef().IsSame(srm))
	nwkt, _ = gp.WKT()
	assert.NotEqual(t, "POINT (10 10)", nwkt)
	gp.Close()

	ehc = eh()
	gp, _ = NewGeometryFromWKT("POINT (10 10)", nil)
	err = gp.Transform(trn, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	gp.Close()

	gp, _ = NewGeometryFromWKT("POINT (10 91)", sr)
	err = gp.Transform(trn)
	assert.Error(t, err)
	gp.Close()

	ehc = eh()
	gp, _ = NewGeometryFromWKT("POINT (10 91)", sr)
	err = gp.Transform(trn, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	gp.Close()
}

func TestMultiPolygonGeometry(t *testing.T) {
	sr, _ := NewSpatialRefFromEPSG(4326)
	defer sr.Close()

	multiPolyStr := "MULTIPOLYGON(((1 1,5 1,5 5,1 5,1 1),(2 2,2 3,3 3,3 2,2 2)),((6 3,9 2,9 4,6 3)))"
	multiPolyGeom, _ := NewGeometryFromWKT(multiPolyStr, sr)

	assert.Equal(t, multiPolyGeom.Area(), 18.0)
	assert.Equal(t, multiPolyGeom.GeometryCount(), 2)
	assert.Equal(t, multiPolyGeom.Type(), GTMultiPolygon)

	subGeom, err := multiPolyGeom.SubGeometry(0)
	assert.NoError(t, err)
	wkt, _ := subGeom.WKT()
	assert.Equal(t, wkt, "POLYGON ((1 1,5 1,5 5,1 5,1 1),(2 2,2 3,3 3,3 2,2 2))")
	subGeom, err = multiPolyGeom.SubGeometry(1)
	assert.NoError(t, err)
	wkt, _ = subGeom.WKT()
	assert.Equal(t, wkt, "POLYGON ((6 3,9 2,9 4,6 3))")
	ehc := eh()
	_, err = multiPolyGeom.SubGeometry(2, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	polyGeom := multiPolyGeom.ForceToPolygon()
	wkt, _ = polyGeom.WKT()
	assert.Equal(t, wkt, "POLYGON ((1 1,5 1,5 5,1 5,1 1),(2 2,2 3,3 3,3 2,2 2),(6 3,9 2,9 4,6 3))")
	assert.False(t, polyGeom.Valid())

	polyStr := "POLYGON((1 1,5 1,5 5,1 5,1 1))"
	polyGeom, _ = NewGeometryFromWKT(polyStr, sr)
	multiPolyGeom = polyGeom.ForceToMultiPolygon()
	wkt, _ = multiPolyGeom.WKT()
	assert.Equal(t, wkt, "MULTIPOLYGON (((1 1,5 1,5 5,1 5,1 1)))")
	assert.True(t, polyGeom.Valid())

	multiPolyStr = "MULTIPOLYGON (((1 1,5 1,5 5,1 5,1 1)))"
	multiPolyGeom, _ = NewGeometryFromWKT(multiPolyStr, sr)
	polyStr = "POLYGON((6 3,9 2,9 4,6 3))"
	polyGeom, _ = NewGeometryFromWKT(polyStr, sr)
	assert.False(t, multiPolyGeom.Contains(polyGeom))
	err = multiPolyGeom.AddGeometry(polyGeom, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	wkt, _ = multiPolyGeom.WKT()
	assert.Equal(t, wkt, "MULTIPOLYGON (((1 1,5 1,5 5,1 5,1 1)),((6 3,9 2,9 4,6 3)))")
}

func TestFeatureAttributes(t *testing.T) {
	glayers := `{
	"type": "FeatureCollection",
	"features": [
		{
			"type": "Feature",
			"properties": {
				"strCol":"foobar",
				"intCol":3,
				"floatCol":123.4
			},
			"geometry": {
				"type": "Point",
				"coordinates": [1,1]
			}
		}
	]
}`
	ds, _ := Open(glayers, VectorOnly())
	lyr := ds.Layers()[0]

	//trying to make this fail "cleanly", but not managing. using a null layer for this
	//curve, err := NewGeometryFromWKT("CURVEPOLYGON(COMPOUNDCURVE(CIRCULARSTRING (0 0,1 1,2 0),(2 0,0 0)))", nil)
	//assert.NoError(t, err)
	_, err := (&Layer{}).NewFeature(&Geometry{})
	assert.Error(t, err)
	ehc := eh()
	_, err = (&Layer{}).NewFeature(&Geometry{}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	f := lyr.NextFeature()
	assert.NotNil(t, f)
	fields := f.Fields()
	_, err = fields.GetByName("foo")
	assert.Error(t, err)
	_, err = fields.GetByIndex(-1)
	assert.Error(t, err)
	strField, err := fields.GetByName("strCol")
	assert.NoError(t, err)
	fvqs := NewFieldValueQuerier[string](f)
	strVal, err := fvqs.GetValue(strField)
	assert.NoError(t, err)
	assert.Equal(t, "foobar", strVal)
	strField, err = fields.GetByIndex(0)
	assert.NoError(t, err)
	strVal, err = fvqs.GetValue(strField)
	assert.NoError(t, err)
	assert.Equal(t, "foobar", strVal)
	intField, err := fields.GetByName("intCol")
	assert.NoError(t, err)
	fvqi := NewFieldValueQuerier[int](f)
	intVal, err := fvqi.GetValue(intField)
	assert.NoError(t, err)
	assert.Equal(t, 3, intVal)
	floatField, err := fields.GetByName("floatCol")
	assert.NoError(t, err)
	fvqf := NewFieldValueQuerier[float64](f)
	floatVal, err := fvqf.GetValue(floatField)
	assert.NoError(t, err)
	assert.Equal(t, 123.4, floatVal)
	_, err = fvqf.GetValue(strField)
	assert.Error(t, err)
	_ = ds.Close()

	ds, _ = CreateVector(Memory, "")
	lyr, err = ds.CreateLayer("l1", nil, GTPoint,
		NewFieldDefinition("strCol", FTString),
		NewFieldDefinition("intCol", FTInt),
		NewFieldDefinition("int64Col", FTInt64),
		NewFieldDefinition("floatCol", FTReal),
		NewFieldDefinition("intListCol", FTIntList),
		NewFieldDefinition("int64ListCol", FTInt64List),
		NewFieldDefinition("floatListCol", FTRealList),
		NewFieldDefinition("stringListCol", FTStringList),
		NewFieldDefinition("binaryCol", FTBinary),
		NewFieldDefinition("dateCol", FTDate),
		NewFieldDefinition("timeCol", FTTime),
		NewFieldDefinition("dateTimeCol", FTDateTime),
		NewFieldDefinition("unknownCol", FTUnknown),
	)
	assert.NoError(t, err)

	pnt, _ := NewGeometryFromWKT("POINT (1 1)", nil)
	nf, err := lyr.NewFeature(pnt)
	assert.NoError(t, err)
	fc, _ := lyr.FeatureCount()
	assert.Equal(t, fc, 1)
	nf.SetGeometryColumnName("no_error")
	nf.SetFID(0)
	fields = nf.Fields()
	fvqs = NewFieldValueQuerier[string](nf)
	fvqi = NewFieldValueQuerier[int](nf)
	fvqi64 := NewFieldValueQuerier[int64](nf)
	fvqf = NewFieldValueQuerier[float64](nf)
	fvqil := NewFieldValueQuerier[[]int](nf)
	fvqi64l := NewFieldValueQuerier[[]int64](nf)
	fvqfl := NewFieldValueQuerier[[]float64](nf)
	fvqsl := NewFieldValueQuerier[[]string](nf)
	fvqb := NewFieldValueQuerier[[]byte](nf)
	fvqt := NewFieldValueQuerier[time.Time](nf)
	strField, _ = fields.GetByName("strCol")
	intField, _ = fields.GetByName("intCol")
	int64Field, _ := fields.GetByName("int64Col")
	floatField, _ = fields.GetByName("floatCol")
	intListField, _ := fields.GetByName("intListCol")
	int64ListField, _ := fields.GetByName("int64ListCol")
	floatListField, _ := fields.GetByName("floatListCol")
	strListField, _ := fields.GetByName("stringListCol")
	binaryField, _ := fields.GetByName("binaryCol")
	dateField, _ := fields.GetByName("dateCol")
	timeField, _ := fields.GetByName("timeCol")
	dateTimeField, _ := fields.GetByName("dateTimeCol")
	unknownField, _ := fields.GetByName("unknownCol")
	assert.Error(t, fvqi.SetValue(strField, 0))
	assert.Error(t, fvqs.SetValue(intField, ""))
	assert.Error(t, fvqs.SetValue(int64Field, ""))
	assert.Error(t, fvqs.SetValue(floatField, ""))
	assert.Error(t, fvqs.SetValue(intListField, ""))
	assert.Error(t, fvqs.SetValue(int64ListField, ""))
	assert.Error(t, fvqs.SetValue(floatListField, ""))
	assert.Error(t, fvqs.SetValue(strListField, ""))
	assert.Error(t, fvqs.SetValue(binaryField, ""))
	assert.Error(t, fvqs.SetValue(dateField, ""))
	assert.Error(t, fvqs.SetValue(timeField, ""))
	assert.Error(t, fvqs.SetValue(dateTimeField, ""))
	assert.Error(t, fvqs.SetValue(unknownField, ""))
	assert.NoError(t, fvqs.SetValue(strField, "foo"))
	assert.NoError(t, fvqi.SetValue(intField, 1))
	assert.NoError(t, fvqi64.SetValue(int64Field, int64(2)))
	assert.NoError(t, fvqf.SetValue(floatField, 3.0))
	assert.NoError(t, fvqil.SetValue(intListField, []int{1, 2, 3}))
	assert.NoError(t, fvqi64l.SetValue(int64ListField, []int64{1, 2, 3}))
	assert.NoError(t, fvqfl.SetValue(floatListField, []float64{1, 2, 3}))
	assert.NoError(t, fvqsl.SetValue(strListField, []string{"1", "2", "3"}))
	assert.NoError(t, fvqb.SetValue(binaryField, []byte("foo")))
	date := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.NoError(t, fvqt.SetValue(dateField, date))
	assert.NoError(t, fvqt.SetValue(timeField, date))
	assert.NoError(t, fvqt.SetValue(dateTimeField, date))
	// Reload fields from feature to check if they have been properly set
	fields = nf.Fields()
	strField, _ = fields.GetByName("strCol")
	assert.True(t, strField.IsSet())
	assert.Equal(t, FTString, strField.Type())
	strVal, _ = fvqs.GetValue(strField)
	assert.Equal(t, "foo", strVal)
	intField, _ = fields.GetByName("intCol")
	assert.Equal(t, FTInt, intField.Type())
	intVal, _ = fvqi.GetValue(intField)
	assert.Equal(t, 1, intVal)
	int64Field, _ = fields.GetByName("int64Col")
	assert.Equal(t, FTInt64, int64Field.Type())
	int64Val, _ := fvqi64.GetValue(int64Field)
	assert.Equal(t, int64(2), int64Val)
	floatField, _ = fields.GetByName("floatCol")
	assert.Equal(t, FTReal, floatField.Type())
	floatVal, _ = fvqf.GetValue(floatField)
	assert.Equal(t, 3.0, floatVal)
	intListField, _ = fields.GetByName("intListCol")
	assert.Equal(t, FTIntList, intListField.Type())
	intListVal, _ := fvqil.GetValue(intListField)
	assert.Equal(t, []int{1, 2, 3}, intListVal)
	int64ListField, _ = fields.GetByName("int64ListCol")
	assert.Equal(t, FTInt64List, int64ListField.Type())
	int64ListVal, _ := fvqi64l.GetValue(int64ListField)
	assert.Equal(t, []int64{1, 2, 3}, int64ListVal)
	floatListField, _ = fields.GetByName("floatListCol")
	assert.Equal(t, FTRealList, floatListField.Type())
	floatListVal, _ := fvqfl.GetValue(floatListField)
	assert.Equal(t, []float64{1, 2, 3}, floatListVal)
	strListField, _ = fields.GetByName("stringListCol")
	assert.Equal(t, FTStringList, strListField.Type())
	strListVal, _ := fvqsl.GetValue(strListField)
	assert.Equal(t, []string{"1", "2", "3"}, strListVal)
	binaryField, _ = fields.GetByName("binaryCol")
	assert.Equal(t, FTBinary, binaryField.Type())
	binaryVal, _ := fvqb.GetValue(binaryField)
	assert.Equal(t, []byte("foo"), binaryVal)
	dateField, _ = fields.GetByName("dateCol")
	assert.Equal(t, FTDate, dateField.Type())
	timeVal, _ := fvqt.GetValue(dateField)
	assert.Equal(t, date, timeVal)
	timeField, _ = fields.GetByName("timeCol")
	assert.Equal(t, FTTime, timeField.Type())
	timeVal, _ = fvqt.GetValue(timeField)
	assert.Equal(t, date, timeVal)
	dateTimeField, _ = fields.GetByName("dateTimeCol")
	assert.Equal(t, FTDateTime, dateTimeField.Type())
	timeVal, _ = fvqt.GetValue(dateTimeField)
	assert.Equal(t, date, timeVal)

	fvqs = NewFieldValueQuerier[string](nil)
	assert.Error(t, fvqs.SetValue(strField, "foo"))
	fvqs = NewFieldValueQuerier[string](nf)
	nf.Close()
	assert.Error(t, fvqs.SetValue(strField, "foo"))

	nf, err = lyr.NewFeature(nil)
	assert.NoError(t, err)
	g := nf.Geometry()
	assert.True(t, g.Empty())

	_ = ds.Close()

	/* attempt at raising an error
	RegisterVector(Mitab)
	tmpdir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(tmpdir)
	ds, err = CreateVector(Mitab, filepath.Join(tmpdir, "data.tab"))
	assert.NoError(t, err)
	lyr, err = ds.CreateLayer("l1", nil, GTPoint,
		NewFieldDefinition("strCol", FTString),
	)
	assert.NoError(t, err)
	line, err := NewGeometryFromWKT("LINESTRING (1 1,2 2)", nil)
	assert.NoError(t, err)
	nf, err = lyr.NewFeature(line)
	assert.Error(t, err)
	*/
	ds, _ = CreateVector(Memory, "")
	lyr, err = ds.CreateLayer("l1", nil, GTPoint)
	assert.NoError(t, err)

	pnt, _ = NewGeometryFromWKT("POINT (1 1)", nil)
	nf, _ = lyr.NewFeature(pnt)
	fields = nf.Fields()
	assert.Len(t, fields, 0)
}

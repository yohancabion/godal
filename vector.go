//go:build go1.18
// +build go1.18

package godal

/*
#include "godal.h"
#include <stdlib.h>

#cgo pkg-config: gdal
#cgo CXXFLAGS: -std=c++11
#cgo LDFLAGS: -ldl
*/
import "C"
import (
	"errors"
	"fmt"
	"time"
	"unsafe"
)

// Polygonize wraps GDALPolygonize
func (band Band) Polygonize(dstLayer Layer, opts ...PolygonizeOption) error {
	popt := polygonizeOpts{
		pixFieldIndex: -1,
	}
	maskBand := band.MaskBand()
	popt.mask = &maskBand

	for _, opt := range opts {
		opt.setPolygonizeOpt(&popt)
	}
	copts := sliceToCStringArray(popt.options)
	defer copts.free()
	var cMaskBand C.GDALRasterBandH = nil
	if popt.mask != nil {
		cMaskBand = popt.mask.handle()
	}

	cgc := createCGOContext(nil, popt.errorHandler)
	C.godalPolygonize(cgc.cPointer(), band.handle(), cMaskBand, dstLayer.handle(), C.int(popt.pixFieldIndex), copts.cPointer())
	return cgc.close()
}

// Rasterize wraps GDALRasterize()
func (ds *Dataset) Rasterize(dstDS string, switches []string, opts ...RasterizeOption) (*Dataset, error) {
	gopts := rasterizeOpts{}
	for _, opt := range opts {
		opt.setRasterizeOpt(&gopts)
	}
	for _, copt := range gopts.create {
		switches = append(switches, "-co", copt)
	}
	if gopts.driver != "" {
		dname := string(gopts.driver)
		if dm, ok := driverMappings[gopts.driver]; ok {
			dname = dm.rasterName
		}
		switches = append(switches, "-of", dname)
	}
	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()
	cname := unsafe.Pointer(C.CString(dstDS))
	defer C.free(cname)

	cgc := createCGOContext(gopts.config, gopts.errorHandler)
	hndl := C.godalRasterize(cgc.cPointer(), (*C.char)(cname), ds.handle(), cswitches.cPointer())
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// RasterizeGeometry "burns" the provided geometry onto ds.
// By default, the "0" value is burned into all of ds's bands. This behavior can be modified
// with the following options:
//  - Bands(bnd ...int) the list of bands to affect
//  - Values(val ...float64) the pixel value to burn. There must be either 1 or len(bands) values
// provided
//  - AllTouched() pixels touched by lines or polygons will be updated, not just those on the line
// render path, or whose center point is within the polygon.
//
func (ds *Dataset) RasterizeGeometry(g *Geometry, opts ...RasterizeGeometryOption) error {
	opt := rasterizeGeometryOpts{}
	for _, o := range opts {
		o.setRasterizeGeometryOpt(&opt)
	}
	if len(opt.bands) == 0 {
		bnds := ds.Bands()
		opt.bands = make([]int, len(bnds))
		for i := range bnds {
			opt.bands[i] = i + 1
		}
	}
	if len(opt.values) == 0 {
		opt.values = make([]float64, len(opt.bands))
		for i := range opt.values {
			opt.values[i] = 0
		}
	}
	if len(opt.values) == 1 && len(opt.values) != len(opt.bands) {
		for i := 1; i < len(opt.bands); i++ {
			opt.values = append(opt.values, opt.values[0])
		}
	}
	if len(opt.values) != len(opt.bands) {
		return fmt.Errorf("must pass in same number of values as bands")
	}
	cgc := createCGOContext(nil, opt.errorHandler)
	C.godalRasterizeGeometry(cgc.cPointer(), ds.handle(), g.handle,
		cIntArray(opt.bands), C.int(len(opt.bands)), cDoubleArray(opt.values), C.int(opt.allTouched))
	return cgc.close()
}

// GeometryType is a geometry type
type GeometryType uint32

const (
	//GTUnknown is a GeometryType
	GTUnknown = GeometryType(C.wkbUnknown)
	//GTPoint is a GeometryType
	GTPoint = GeometryType(C.wkbPoint)
	//GTPoint25D is a GeometryType
	GTPoint25D = GeometryType(C.wkbPoint25D)
	//GTLinearRing is a GeometryType
	GTLinearRing = GeometryType(C.wkbLinearRing)
	//GTLineString is a GeometryType
	GTLineString = GeometryType(C.wkbLineString)
	//GTLineString25D is a GeometryType
	GTLineString25D = GeometryType(C.wkbLineString25D)
	//GTPolygon is a GeometryType
	GTPolygon = GeometryType(C.wkbPolygon)
	//GTPolygon25D is a GeometryType
	GTPolygon25D = GeometryType(C.wkbPolygon25D)
	//GTMultiPoint is a GeometryType
	GTMultiPoint = GeometryType(C.wkbMultiPoint)
	//GTMultiPoint25D is a GeometryType
	GTMultiPoint25D = GeometryType(C.wkbMultiPoint25D)
	//GTMultiLineString is a GeometryType
	GTMultiLineString = GeometryType(C.wkbMultiLineString)
	//GTMultiLineString25D is a GeometryType
	GTMultiLineString25D = GeometryType(C.wkbMultiLineString25D)
	//GTMultiPolygon is a GeometryType
	GTMultiPolygon = GeometryType(C.wkbMultiPolygon)
	//GTMultiPolygon25D is a GeometryType
	GTMultiPolygon25D = GeometryType(C.wkbMultiPolygon25D)
	//GTGeometryCollection is a GeometryType
	GTGeometryCollection = GeometryType(C.wkbGeometryCollection)
	//GTGeometryCollection25D is a GeometryType
	GTGeometryCollection25D = GeometryType(C.wkbGeometryCollection25D)
	//GTNone is a GeometryType
	GTNone = GeometryType(C.wkbNone)
)

//NewFieldDefinition creates a FieldDefinition
func NewFieldDefinition(name string, fdtype FieldType) *FieldDefinition {
	return &FieldDefinition{
		name:  name,
		ftype: fdtype,
	}
}

func (fd *FieldDefinition) setCreateLayerOpt(o *createLayerOpts) {
	o.fields = append(o.fields, fd)
}

func (fd *FieldDefinition) createHandle() C.OGRFieldDefnH {
	cfname := unsafe.Pointer(C.CString(fd.name))
	defer C.free(cfname)
	cfd := C.OGR_Fld_Create((*C.char)(cfname), C.OGRFieldType(fd.ftype))
	return cfd
}

// VectorTranslate runs the library version of ogr2ogr
// See the ogr2ogr doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches :
//  []string{
//    "-f", "GeoJSON",
//	  "-t_srs","epsg:3857",
//    "-dstalpha"}
//
// Creation options and Driver may be set either in the switches slice with
//  switches:=[]string{"-dsco","TILED=YES", "-f","GeoJSON"}
// or through Options with
//  ds.VectorTranslate(dst, switches, CreationOption("TILED=YES","BLOCKXSIZE=256"), GeoJSON)
func (ds *Dataset) VectorTranslate(dstDS string, switches []string, opts ...DatasetVectorTranslateOption) (*Dataset, error) {
	gopts := dsVectorTranslateOpts{}
	for _, opt := range opts {
		opt.setDatasetVectorTranslateOpt(&gopts)
	}
	for _, copt := range gopts.creation {
		switches = append(switches, "-dsco", copt)
	}
	if gopts.driver != "" {
		dname := string(gopts.driver)
		if dm, ok := driverMappings[gopts.driver]; ok {
			dname = dm.vectorName
		}
		switches = append(switches, "-f", dname)
	}
	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()
	cname := unsafe.Pointer(C.CString(dstDS))
	defer C.free(cname)

	cgc := createCGOContext(gopts.config, gopts.errorHandler)
	hndl := C.godalDatasetVectorTranslate(cgc.cPointer(), (*C.char)(cname), ds.handle(), cswitches.cPointer())
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// Layer wraps an OGRLayerH
type Layer struct {
	majorObject
}

// handle returns a pointer to the underlying GDALRasterBandH
func (layer Layer) handle() C.OGRLayerH {
	return C.OGRLayerH(layer.majorObject.cHandle)
}

// Name returns the layer name
func (layer Layer) Name() string {
	return C.GoString(C.OGR_L_GetName(layer.handle()))
}

// Type returns the layer geometry type.
func (layer Layer) Type() GeometryType {
	return GeometryType(C.OGR_L_GetGeomType(layer.handle()))
}

//Bounds returns the layer's envelope in the order minx,miny,maxx,maxy
func (layer Layer) Bounds(opts ...BoundsOption) ([4]float64, error) {
	bo := boundsOpts{}
	for _, o := range opts {
		o.setBoundsOpt(&bo)
	}
	var env C.OGREnvelope
	cgc := createCGOContext(nil, bo.errorHandler)
	C.godalLayerGetExtent(cgc.cPointer(), layer.handle(), &env)
	if err := cgc.close(); err != nil {
		return [4]float64{}, err
	}
	bnds := [4]float64{
		float64(env.MinX),
		float64(env.MinY),
		float64(env.MaxX),
		float64(env.MaxY),
	}
	if bo.sr == nil {
		return bnds, nil
	}
	sr := layer.SpatialRef()
	defer sr.Close()
	bnds, err := reprojectBounds(bnds, sr, bo.sr)
	if err != nil {
		return [4]float64{}, err
	}
	return bnds, nil
}

// FeatureCount returns the number of features in the layer
func (layer Layer) FeatureCount(opts ...FeatureCountOption) (int, error) {
	fco := &featureCountOpts{}
	for _, o := range opts {
		o.setFeatureCountOpt(fco)
	}
	var count C.int
	cgc := createCGOContext(nil, fco.errorHandler)
	C.godalLayerFeatureCount(cgc.cPointer(), layer.handle(), &count)
	if err := cgc.close(); err != nil {
		return 0, err
	}
	return int(count), nil
}

// Layers returns all dataset layers
func (ds *Dataset) Layers() []Layer {
	clayers := C.godalVectorLayers(ds.handle())
	if clayers == nil {
		return nil
	}
	defer C.free(unsafe.Pointer(clayers))
	//https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
	sLayers := (*[1 << 30]C.OGRLayerH)(unsafe.Pointer(clayers))
	layers := []Layer{}
	i := 0
	for {
		if sLayers[i] == nil {
			return layers
		}
		layers = append(layers, Layer{majorObject{C.GDALMajorObjectH(sLayers[i])}})
		i++
	}
}

// SpatialRef returns dataset projection.
func (layer Layer) SpatialRef() *SpatialRef {
	hndl := C.OGR_L_GetSpatialRef(layer.handle())
	return &SpatialRef{handle: hndl, isOwned: false}
}

// Geometry wraps a OGRGeometryH
type Geometry struct {
	isOwned bool
	handle  C.OGRGeometryH
}

// Area computes the area for geometries of type LinearRing, Polygon or MultiPolygon (returns zero for other types).
// The area is in square units of the spatial reference system in use.
func (g *Geometry) Area() float64 {
	return float64(C.OGR_G_Area(g.handle))
}

// GeometryCount fetch the number of elements in a geometry or number of geometries in container.
// Only geometries of type Polygon, MultiPoint, MultiLineString, MultiPolygon or GeometryCollection may return a valid value.
// Other geometry types will silently return 0.
// For a polygon, the returned number is the number of rings (exterior ring + interior rings).
func (g *Geometry) GeometryCount() int {
	return int(C.OGR_G_GetGeometryCount(g.handle))
}

// Type fetch geometry type.
func (g *Geometry) Type() GeometryType {
	return GeometryType(C.OGR_G_GetGeometryType(g.handle))
}

//Simplify simplifies the geometry with the given tolerance
func (g *Geometry) Simplify(tolerance float64, opts ...SimplifyOption) (*Geometry, error) {
	so := &simplifyOpts{}
	for _, o := range opts {
		o.setSimplifyOpt(so)
	}
	cgc := createCGOContext(nil, so.errorHandler)
	hndl := C.godal_OGR_G_Simplify(cgc.cPointer(), g.handle, C.double(tolerance))
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}, nil
}

//Buffer buffers the geometry
func (g *Geometry) Buffer(distance float64, segments int, opts ...BufferOption) (*Geometry, error) {
	bo := &bufferOpts{}
	for _, o := range opts {
		o.setBufferOpt(bo)
	}
	cgc := createCGOContext(nil, bo.errorHandler)
	hndl := C.godal_OGR_G_Buffer(cgc.cPointer(), g.handle, C.double(distance), C.int(segments))
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}, nil
}

// Difference generates a new geometry which is the region of this geometry with the region of the other geometry removed.
func (g *Geometry) Difference(other *Geometry, opts ...DifferenceOption) (*Geometry, error) {
	// If other geometry is nil, GDAL crashes
	if other == nil || other.handle == nil {
		return nil, errors.New("other geometry is empty")
	}
	do := &differenceOpts{}
	for _, o := range opts {
		o.setDifferenceOpt(do)
	}
	cgc := createCGOContext(nil, do.errorHandler)
	hndl := C.godal_OGR_G_Difference(cgc.cPointer(), g.handle, other.handle)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}, nil
}

// AddGeometry add a geometry to a geometry container.
func (g *Geometry) AddGeometry(subGeom *Geometry, opts ...AddGeometryOption) error {
	ago := &addGeometryOpts{}
	for _, o := range opts {
		o.setAddGeometryOpt(ago)
	}
	cgc := createCGOContext(nil, ago.errorHandler)
	C.godal_OGR_G_AddGeometry(cgc.cPointer(), g.handle, subGeom.handle)
	return cgc.close()
}

// ForceToMultiPolygon convert to multipolygon.
func (g *Geometry) ForceToMultiPolygon() *Geometry {
	hndl := C.OGR_G_ForceToMultiPolygon(g.handle)
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}
}

// ForceToPolygon convert to polygon.
func (g *Geometry) ForceToPolygon() *Geometry {
	hndl := C.OGR_G_ForceToPolygon(g.handle)
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}
}

// SubGeometry Fetch geometry from a geometry container.
func (g *Geometry) SubGeometry(subGeomIndex int, opts ...SubGeometryOption) (*Geometry, error) {
	so := &subGeometryOpts{}
	for _, o := range opts {
		o.setSubGeometryOpt(so)
	}
	cgc := createCGOContext(nil, so.errorHandler)
	hndl := C.godal_OGR_G_GetGeometryRef(cgc.cPointer(), g.handle, C.int(subGeomIndex))
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{
		isOwned: false,
		handle:  hndl,
	}, nil
}

// Intersects determines whether two geometries intersect. If GEOS is enabled, then
// this is done in rigorous fashion otherwise TRUE is returned if the
// envelopes (bounding boxes) of the two geometries overlap.
func (g *Geometry) Intersects(other *Geometry, opts ...IntersectsOption) (bool, error) {
	bo := &intersectsOpts{}
	for _, o := range opts {
		o.setIntersectsOpt(bo)
	}
	cgc := createCGOContext(nil, bo.errorHandler)
	ret := C.godal_OGR_G_Intersects(cgc.cPointer(), g.handle, other.handle)
	if err := cgc.close(); err != nil {
		return false, err
	}
	return ret != 0, nil
}

// Union generates a new geometry which is the region of union of the two geometries operated on.
func (g *Geometry) Union(other *Geometry, opts ...UnionOption) (*Geometry, error) {
	// If other geometry is nil, GDAL crashes
	if other == nil || other.handle == nil {
		return nil, errors.New("other geometry is empty")
	}
	uo := &unionOpts{}
	for _, o := range opts {
		o.setUnionOpt(uo)
	}
	cgc := createCGOContext(nil, uo.errorHandler)
	hndl := C.godal_OGR_G_Union(cgc.cPointer(), g.handle, other.handle)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}, nil
}

//Contains tests if this geometry contains the other geometry.
func (g *Geometry) Contains(other *Geometry) bool {
	ret := C.OGR_G_Contains(g.handle, other.handle)
	return ret != 0
}

//Empty returns true if the geometry is empty
func (g *Geometry) Empty() bool {
	ret := C.OGR_G_IsEmpty(g.handle)
	return ret != 0
}

//Valid returns true is the geometry is valid
func (g *Geometry) Valid() bool {
	ret := C.OGR_G_IsValid(g.handle)
	return ret != 0
}

//Bounds returns the geometry's envelope in the order minx,miny,maxx,maxy
func (g *Geometry) Bounds(opts ...BoundsOption) ([4]float64, error) {
	bo := boundsOpts{}
	for _, o := range opts {
		o.setBoundsOpt(&bo)
	}
	var env C.OGREnvelope
	C.OGR_G_GetEnvelope(g.handle, &env)
	bnds := [4]float64{
		float64(env.MinX),
		float64(env.MinY),
		float64(env.MaxX),
		float64(env.MaxY),
	}
	if bo.sr == nil {
		return bnds, nil
	}
	sr := g.SpatialRef()
	defer sr.Close()
	ret, err := reprojectBounds(bnds, sr, bo.sr)
	if err != nil {
		return bnds, err
	}
	return ret, nil
}

// Close may reclaim memory from geometry. Must be called exactly once.
func (g *Geometry) Close() {
	if g.handle == nil {
		return
		//panic("geometry already closed")
	}
	if g.isOwned {
		C.OGR_G_DestroyGeometry(g.handle)
	}
	g.handle = nil
}

//Feature is a Layer feature
type Feature struct {
	handle C.OGRFeatureH
}

//Geometry returns a handle to the feature's geometry
func (f *Feature) Geometry() *Geometry {
	hndl := C.OGR_F_GetGeometryRef(f.handle)
	return &Geometry{
		isOwned: false,
		handle:  hndl,
	}
}

//SetGeometry overwrites the feature's geometry
func (f *Feature) SetGeometry(geom *Geometry, opts ...SetGeometryOption) error {
	sgo := &setGeometryOpts{}
	for _, o := range opts {
		o.setSetGeometryOpt(sgo)
	}
	cgc := createCGOContext(nil, sgo.errorHandler)
	C.godalFeatureSetGeometry(cgc.cPointer(), f.handle, geom.handle)
	return cgc.close()
}

//SetGeometryColumnName set the name of feature first geometry field.
func (f *Feature) SetGeometryColumnName(name string) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	gfdef := C.OGR_F_GetGeomFieldDefnRef(f.handle, C.int(0))
	if gfdef != nil {
		C.OGR_GFld_SetName(gfdef, (*C.char)(unsafe.Pointer(cname)))
	}
}

//SetFID set feature identifier
func (f *Feature) SetFID(fid int) {
	// OGR error returned is always none, so we don't handle it
	C.OGR_F_SetFID(f.handle, C.GIntBig(fid))
}

//Close releases resources associated to a feature
func (f *Feature) Close() {
	if f.handle == nil {
		return
		//panic("feature closed more than once")
	}
	C.OGR_F_Destroy(f.handle)
	f.handle = nil
}

//Fields represent all Feature attributes
type Fields []Field

//Field is a Feature attribute
type Field struct {
	index int
	name  string
	isSet bool
	ftype FieldType
	val   any
}

//Fields returns all the Feature's fields
func (f *Feature) Fields() Fields {
	fcount := C.OGR_F_GetFieldCount(f.handle)
	if fcount == 0 {
		return nil
	}
	fields := make([]Field, int(fcount))
	for fid := C.int(0); fid < fcount; fid++ {
		fdefn := C.OGR_F_GetFieldDefnRef(f.handle, fid)
		fname := C.GoString(C.OGR_Fld_GetNameRef(fdefn))
		ftype := C.OGR_Fld_GetType(fdefn)
		fld := Field{
			index: int(fid),
			name:  fname,
			isSet: C.OGR_F_IsFieldSet(f.handle, fid) != 0,
		}
		switch ftype {
		case C.OFTInteger:
			fld.ftype = FTInt
			fld.val = int(C.OGR_F_GetFieldAsInteger64(f.handle, fid))
		case C.OFTInteger64:
			fld.ftype = FTInt64
			fld.val = int64(C.OGR_F_GetFieldAsInteger64(f.handle, fid))
		case C.OFTReal:
			fld.ftype = FTReal
			fld.val = float64(C.OGR_F_GetFieldAsDouble(f.handle, fid))
		case C.OFTString:
			fld.ftype = FTString
			fld.val = C.GoString(C.OGR_F_GetFieldAsString(f.handle, fid))
		case C.OFTDate:
			fld.ftype = FTDate
			fld.val = f.getFieldAsDateTime(fid)
		case C.OFTTime:
			fld.ftype = FTTime
			fld.val = f.getFieldAsDateTime(fid)
		case C.OFTDateTime:
			fld.ftype = FTDateTime
			fld.val = f.getFieldAsDateTime(fid)
		case C.OFTIntegerList:
			fld.ftype = FTIntList
			var length C.int
			cArray := C.OGR_F_GetFieldAsIntegerList(f.handle, fid, &length)
			fld.val = cIntArrayToSlice(cArray, length)
		case C.OFTInteger64List:
			fld.ftype = FTInt64List
			var length C.int
			cArray := C.OGR_F_GetFieldAsInteger64List(f.handle, fid, &length)
			fld.val = cLongArrayToSlice(cArray, length)
		case C.OFTRealList:
			fld.ftype = FTRealList
			var length C.int
			cArray := C.OGR_F_GetFieldAsDoubleList(f.handle, fid, &length)
			fld.val = cDoubleArrayToSlice(cArray, length)
		case C.OFTStringList:
			fld.ftype = FTStringList
			cArray := C.OGR_F_GetFieldAsStringList(f.handle, fid)
			fld.val = cStringArrayToSlice(cArray)
		case C.OFTBinary:
			fld.ftype = FTBinary
			var length C.int
			cArray := C.OGR_F_GetFieldAsBinary(f.handle, fid, &length)
			var slice []byte
			if cArray != nil {
				slice = C.GoBytes(unsafe.Pointer(cArray), length)
			}
			fld.val = slice
		default:
			// Only deprecated field types like FTWideString & WideStringList should be handled by default case
			fld.ftype = FTUnknown
		}
		fields[int(fid)] = fld
	}

	return fields
}

//Fetch field as date and time
func (f *Feature) getFieldAsDateTime(index C.int) time.Time {
	var year, month, day, hour, minute, second, tzFlag int
	ret := C.OGR_F_GetFieldAsDateTime(
		f.handle,
		index,
		(*C.int)(unsafe.Pointer(&year)),
		(*C.int)(unsafe.Pointer(&month)),
		(*C.int)(unsafe.Pointer(&day)),
		(*C.int)(unsafe.Pointer(&hour)),
		(*C.int)(unsafe.Pointer(&minute)),
		(*C.int)(unsafe.Pointer(&second)),
		(*C.int)(unsafe.Pointer(&tzFlag)),
	)
	if ret != 0 {
		// TODO Time zone is not properly handled
		return time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
	}
	return time.Time{}
}

//GetByName returns field with given name
func (flds Fields) GetByName(name string) (*Field, error) {
	for _, field := range flds {
		if field.name == name {
			return &field, nil
		}
	}

	return nil, errors.New("field not found")
}

//GetByIndex returns field at given index
func (flds Fields) GetByIndex(index int) (*Field, error) {
	for _, field := range flds {
		if field.index == index {
			return &field, nil
		}
	}

	return nil, errors.New("field not found")
}

//IsSet returns if the field has ever been assigned a value or not.
func (fld *Field) IsSet() bool {
	return fld.isSet
}

//Type returns the field's native type
func (fld *Field) Type() FieldType {
	return fld.ftype
}

type FieldValueType interface {
	int | int64 | float64 | string | []byte | time.Time | []int | []int64 | []float64 | []string
}

//FieldValueQuerier is a facilitator to access typed field value
type FieldValueQuerier[FVT FieldValueType] struct {
	srcFeature *Feature
}

//NewFieldValueQuerier create a typed FieldValueQuerier
func NewFieldValueQuerier[FVT FieldValueType](srcFeature *Feature) *FieldValueQuerier[FVT] {
	return &FieldValueQuerier[FVT]{
		srcFeature: srcFeature,
	}
}

//GetValue get field value
func (fvq *FieldValueQuerier[FVT]) GetValue(field *Field) (FVT, error) {
	value, ok := field.val.(FVT)
	if !ok {
		var empty FVT
		return empty, errors.New("requested type is not suitable for this type of field")
	}

	return value, nil
}

//SetValue set field value
func (fvq *FieldValueQuerier[FV]) SetValue(field *Field, value FV) error {
	if fvq.srcFeature == nil {
		return errors.New("source feature is nil")
	}
	if fvq.srcFeature.handle == nil {
		return errors.New("invalid source feature, probably closed")
	}

	switch field.ftype {
	case FTInt:
		intValue, ok := any(value).(int)
		if !ok {
			return errors.New("value for this field must be of type 'int'")
		}
		C.OGR_F_SetFieldInteger(fvq.srcFeature.handle, C.int(field.index), C.int(intValue))
	case FTInt64:
		int64Value, ok := any(value).(int64)
		if !ok {
			return errors.New("value for this field must be of type 'int64'")
		}
		C.OGR_F_SetFieldInteger64(fvq.srcFeature.handle, C.int(field.index), C.longlong(int64Value))
	case FTReal:
		floatValue, ok := any(value).(float64)
		if !ok {
			return errors.New("value for this field must be of type 'float64'")
		}
		C.OGR_F_SetFieldDouble(fvq.srcFeature.handle, C.int(field.index), C.double(floatValue))
	case FTString:
		stringValue, ok := any(value).(string)
		if !ok {
			return errors.New("value for this field must be of type 'string'")
		}
		cval := C.CString(stringValue)
		defer C.free(unsafe.Pointer(cval))
		C.OGR_F_SetFieldString(fvq.srcFeature.handle, C.int(field.index), cval)
	case FTDate, FTTime, FTDateTime:
		timeValue, ok := any(value).(time.Time)
		if !ok {
			return errors.New("value for this field must be of type 'time.Time'")
		}
		C.OGR_F_SetFieldDateTime(
			fvq.srcFeature.handle,
			C.int(field.index),
			C.int(timeValue.Year()),
			C.int(timeValue.Month()),
			C.int(timeValue.Day()),
			C.int(timeValue.Hour()),
			C.int(timeValue.Minute()),
			C.int(timeValue.Second()),
			C.int(1), // TODO Time zone is not properly handled
		)
	case FTIntList:
		intListValue, ok := any(value).([]int)
		if !ok {
			return errors.New("value for this field must be of type '[]int'")
		}
		C.OGR_F_SetFieldIntegerList(fvq.srcFeature.handle, C.int(field.index), C.int(len(intListValue)), cIntArray(intListValue))
	case FTInt64List:
		int64ListValue, ok := any(value).([]int64)
		if !ok {
			return errors.New("value for this field must be of type '[]int64'")
		}
		C.OGR_F_SetFieldInteger64List(fvq.srcFeature.handle, C.int(field.index), C.int(len(int64ListValue)), cLongArray(int64ListValue))
	case FTRealList:
		float64ListValue, ok := any(value).([]float64)
		if !ok {
			return errors.New("value for this field must be of type '[]float64'")
		}
		C.OGR_F_SetFieldDoubleList(fvq.srcFeature.handle, C.int(field.index), C.int(len(float64ListValue)), cDoubleArray(float64ListValue))
	case FTStringList:
		stringListValue, ok := any(value).([]string)
		if !ok {
			return errors.New("value for this field must be of type '[]float64'")
		}
		cArray := sliceToCStringArray(stringListValue)
		C.OGR_F_SetFieldStringList(fvq.srcFeature.handle, C.int(field.index), cArray.cPointer())
		cArray.free()
	case FTBinary:
		bytesValue, ok := any(value).([]byte)
		if !ok {
			return errors.New("value for this field must be of type '[]byte'")
		}
		C.OGR_F_SetFieldBinary(fvq.srcFeature.handle, C.int(field.index), C.int(len(bytesValue)), unsafe.Pointer(&bytesValue[0]))
	default:
		return errors.New("setting value is not implemented for this type of field")
	}
	field.isSet = C.OGR_F_IsFieldSet(fvq.srcFeature.handle, C.int(field.index)) != 0
	field.val = value

	return nil
}

// ResetReading makes Layer.NextFeature return the first feature of the layer
func (layer Layer) ResetReading() {
	C.OGR_L_ResetReading(layer.handle())
}

// NextFeature returns the layer's next feature, or nil if there are no mo features
func (layer Layer) NextFeature() *Feature {
	hndl := C.OGR_L_GetNextFeature(layer.handle())
	if hndl == nil {
		return nil
	}
	return &Feature{hndl}
}

// CreateFeature creates a feature on Layer
func (layer Layer) CreateFeature(feat *Feature, opts ...CreateFeatureOption) error {
	cfo := createFeatureOpts{}
	for _, opt := range opts {
		opt.setCreateFeatureOpt(&cfo)
	}
	cgc := createCGOContext(nil, cfo.errorHandler)
	C.godalLayerCreateFeature(cgc.cPointer(), layer.handle(), feat.handle)
	if err := cgc.close(); err != nil {
		return err
	}
	return nil
}

// NewFeature creates a feature on Layer from a geometry
func (layer Layer) NewFeature(geom *Geometry, opts ...NewFeatureOption) (*Feature, error) {
	nfo := newFeatureOpts{}
	for _, opt := range opts {
		opt.setNewFeatureOpt(&nfo)
	}
	ghandle := C.OGRGeometryH(nil)
	if geom != nil {
		ghandle = geom.handle
	}
	cgc := createCGOContext(nil, nfo.errorHandler)
	hndl := C.godalLayerNewFeature(cgc.cPointer(), layer.handle(), ghandle)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Feature{hndl}, nil
}

// UpdateFeature rewrites an updated feature in the Layer
func (layer Layer) UpdateFeature(feat *Feature, opts ...UpdateFeatureOption) error {
	uo := &updateFeatureOpts{}
	for _, o := range opts {
		o.setUpdateFeatureOpt(uo)
	}
	cgc := createCGOContext(nil, uo.errorHandler)
	C.godalLayerSetFeature(cgc.cPointer(), layer.handle(), feat.handle)
	return cgc.close()
}

// DeleteFeature deletes feature from the Layer.
func (layer Layer) DeleteFeature(feat *Feature, opts ...DeleteFeatureOption) error {
	do := &deleteFeatureOpts{}
	for _, o := range opts {
		o.setDeleteFeatureOpt(do)
	}
	cgc := createCGOContext(nil, do.errorHandler)
	C.godalLayerDeleteFeature(cgc.cPointer(), layer.handle(), feat.handle)
	return cgc.close()
}

// CreateLayer creates a new vector layer
//
// Available CreateLayerOptions are
//  - FieldDefinition (may be used multiple times) to add attribute fields to the layer
func (ds *Dataset) CreateLayer(name string, sr *SpatialRef, gtype GeometryType, opts ...CreateLayerOption) (Layer, error) {
	co := createLayerOpts{}
	for _, opt := range opts {
		opt.setCreateLayerOpt(&co)
	}
	srHandle := C.OGRSpatialReferenceH(nil)
	if sr != nil {
		srHandle = sr.handle
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	cgc := createCGOContext(nil, co.errorHandler)
	hndl := C.godalCreateLayer(cgc.cPointer(), ds.handle(), (*C.char)(unsafe.Pointer(cname)), srHandle, C.OGRwkbGeometryType(gtype))
	if err := cgc.close(); err != nil {
		return Layer{}, err
	}
	if len(co.fields) > 0 {
		for _, fld := range co.fields {
			fhndl := fld.createHandle()
			//TODO error checking
			C.OGR_L_CreateField(hndl, fhndl, C.int(0))
			C.OGR_Fld_Destroy(fhndl)
		}
	}
	return Layer{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// CopyLayer Duplicate an existing layer.
func (ds *Dataset) CopyLayer(source Layer, name string, opts ...CopyLayerOption) (Layer, error) {
	co := copyLayerOpts{}
	for _, opt := range opts {
		opt.setCopyLayerOpt(&co)
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	cgc := createCGOContext(nil, co.errorHandler)
	hndl := C.godalCopyLayer(cgc.cPointer(), ds.handle(), source.handle(), (*C.char)(unsafe.Pointer(cname)))
	if err := cgc.close(); err != nil {
		return Layer{}, err
	}
	return Layer{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// LayerByName fetch a layer by name. Returns nil if not found.
func (ds *Dataset) LayerByName(name string) *Layer {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	hndl := C.GDALDatasetGetLayerByName(ds.handle(), (*C.char)(unsafe.Pointer(cname)))
	if hndl == nil {
		return nil
	}
	return &Layer{majorObject{C.GDALMajorObjectH(hndl)}}
}

// NewGeometryFromGeoJSON creates a new Geometry from its GeoJSON representation
func NewGeometryFromGeoJSON(geoJSON string, opts ...NewGeometryOption) (*Geometry, error) {
	no := &newGeometryOpts{}
	for _, o := range opts {
		o.setNewGeometryOpt(no)
	}

	cgeoJSON := C.CString(geoJSON)
	defer C.free(unsafe.Pointer(cgeoJSON))
	cgc := createCGOContext(nil, no.errorHandler)
	hndl := C.godalNewGeometryFromGeoJSON(cgc.cPointer(), (*C.char)(unsafe.Pointer(cgeoJSON)))
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{isOwned: true, handle: hndl}, nil
}

// NewGeometryFromWKT creates a new Geometry from its WKT representation
func NewGeometryFromWKT(wkt string, sr *SpatialRef, opts ...NewGeometryOption) (*Geometry, error) {
	no := &newGeometryOpts{}
	for _, o := range opts {
		o.setNewGeometryOpt(no)
	}
	srHandle := C.OGRSpatialReferenceH(nil)
	if sr != nil {
		srHandle = sr.handle
	}
	cwkt := C.CString(wkt)
	defer C.free(unsafe.Pointer(cwkt))
	cgc := createCGOContext(nil, no.errorHandler)
	hndl := C.godalNewGeometryFromWKT(cgc.cPointer(), (*C.char)(unsafe.Pointer(cwkt)), srHandle)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{isOwned: true, handle: hndl}, nil
}

// NewGeometryFromWKB creates a new Geometry from its WKB representation
func NewGeometryFromWKB(wkb []byte, sr *SpatialRef, opts ...NewGeometryOption) (*Geometry, error) {
	no := &newGeometryOpts{}
	for _, o := range opts {
		o.setNewGeometryOpt(no)
	}
	srHandle := C.OGRSpatialReferenceH(nil)
	if sr != nil {
		srHandle = sr.handle
	}
	cgc := createCGOContext(nil, no.errorHandler)
	hndl := C.godalNewGeometryFromWKB(cgc.cPointer(), unsafe.Pointer(&wkb[0]), C.int(len(wkb)), srHandle)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{isOwned: true, handle: hndl}, nil
}

//WKT returns the Geomtry's WKT representation
func (g *Geometry) WKT(opts ...GeometryWKTOption) (string, error) {
	wo := &geometryWKTOpts{}
	for _, o := range opts {
		o.setGeometryWKTOpt(wo)
	}
	cgc := createCGOContext(nil, wo.errorHandler)
	cwkt := C.godalExportGeometryWKT(cgc.cPointer(), g.handle)
	if err := cgc.close(); err != nil {
		return "", err
	}
	wkt := C.GoString(cwkt)
	C.CPLFree(unsafe.Pointer(cwkt))
	return wkt, nil
}

//WKB returns the Geomtry's WKB representation
func (g *Geometry) WKB(opts ...GeometryWKBOption) ([]byte, error) {
	wo := &geometryWKBOpts{}
	for _, o := range opts {
		o.setGeometryWKBOpt(wo)
	}
	var cwkb unsafe.Pointer
	clen := C.int(0)
	cgc := createCGOContext(nil, wo.errorHandler)
	C.godalExportGeometryWKB(cgc.cPointer(), &cwkb, &clen, g.handle)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	wkb := C.GoBytes(unsafe.Pointer(cwkb), clen)
	C.free(unsafe.Pointer(cwkb))
	return wkb, nil
}

// SpatialRef returns the geometry's SpatialRef
func (g *Geometry) SpatialRef() *SpatialRef {
	hndl := C.OGR_G_GetSpatialReference(g.handle)
	return &SpatialRef{
		handle:  hndl,
		isOwned: false,
	}
}

// SetSpatialRef assigns the given SpatialRef to the Geometry. It does not
// perform an actual reprojection.
func (g *Geometry) SetSpatialRef(sr *SpatialRef) {
	C.OGR_G_AssignSpatialReference(g.handle, sr.handle)
}

// Reproject reprojects the given geometry to the given SpatialRef
func (g *Geometry) Reproject(to *SpatialRef, opts ...GeometryReprojectOption) error {
	gr := &geometryReprojectOpts{}
	for _, o := range opts {
		o.setGeometryReprojectOpt(gr)
	}
	cgc := createCGOContext(nil, gr.errorHandler)
	C.godalGeometryTransformTo(cgc.cPointer(), g.handle, to.handle)
	return cgc.close()
}

// Transform transforms the given geometry. g is expected to already be
// in the supplied Transform source SpatialRef.
func (g *Geometry) Transform(trn *Transform, opts ...GeometryTransformOption) error {
	gt := &geometryTransformOpts{}
	for _, o := range opts {
		o.setGeometryTransformOpt(gt)
	}
	cgc := createCGOContext(nil, gt.errorHandler)
	C.godalGeometryTransform(cgc.cPointer(), g.handle, trn.handle, trn.dst)
	return cgc.close()
}

// GeoJSON returns the geometry in geojson format. The geometry is expected to be in epsg:4326
// projection per RFCxxx
//
// Available GeoJSONOptions are
//  - SignificantDigits(n int) to keep n significant digits after the decimal separator (default: 8)
func (g *Geometry) GeoJSON(opts ...GeoJSONOption) (string, error) {
	gjo := geojsonOpts{
		precision: 7,
	}
	for _, opt := range opts {
		opt.setGeojsonOpt(&gjo)
	}
	cgc := createCGOContext(nil, gjo.errorHandler)
	gjdata := C.godalExportGeometryGeoJSON(cgc.cPointer(), g.handle, C.int(gjo.precision))
	if err := cgc.close(); err != nil {
		return "", err
	}
	wkt := C.GoString(gjdata)
	C.CPLFree(unsafe.Pointer(gjdata))
	return wkt, nil
}

// GML returns the geometry in GML format.
// See the GDAL exportToGML doc page to determine the GML conversion options that can be set through CreationOption.
//
// Example of conversion options :
//  g.GML(CreationOption("FORMAT=GML3","GML3_LONGSRS=YES"))
func (g *Geometry) GML(opts ...GMLExportOption) (string, error) {
	gmlo := &gmlExportOpts{}
	for _, o := range opts {
		o.setGMLExportOpt(gmlo)
	}
	switches := make([]string, len(gmlo.creation))
	for i, copt := range gmlo.creation {
		switches[i] = copt
	}
	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()
	cgc := createCGOContext(nil, gmlo.errorHandler)
	cgml := C.godalExportGeometryGML(cgc.cPointer(), g.handle, cswitches.cPointer())
	if err := cgc.close(); err != nil {
		return "", err
	}
	gml := C.GoString(cgml)
	C.CPLFree(unsafe.Pointer(cgml))
	return gml, nil
}

func cIntArrayToSlice(in *C.int, length C.int) []int {
	if in == nil {
		return nil
	}
	cSlice := (*[1 << 28]C.int)(unsafe.Pointer(in))[:length:length]
	slice := make([]int, length)
	for i, cval := range cSlice {
		slice[i] = int(cval)
	}
	return slice
}

func cLongArrayToSlice(in *C.longlong, length C.int) []int64 {
	if in == nil {
		return nil
	}
	cSlice := (*[1 << 28]C.longlong)(unsafe.Pointer(in))[:length:length]
	slice := make([]int64, length)
	for i, cval := range cSlice {
		slice[i] = int64(cval)
	}
	return slice
}

func cDoubleArrayToSlice(in *C.double, length C.int) []float64 {
	if in == nil {
		return nil
	}
	cSlice := (*[1 << 28]C.double)(unsafe.Pointer(in))[:length:length]
	slice := make([]float64, length)
	for i, cval := range cSlice {
		slice[i] = float64(cval)
	}
	return slice
}

func cLongArray(in []int64) *C.longlong {
	ret := make([]C.longlong, len(in))
	for i := range in {
		ret[i] = C.longlong(in[i])
	}
	return (*C.longlong)(unsafe.Pointer(&ret[0]))
}

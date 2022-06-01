//go:build go1.18
// +build go1.18

// Copyright 2021 Airbus Defence and Space
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package godal_test

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/airbusgeo/godal"
)

// This is the godal port of the official gdal vector tutorial
// located at https://gdal.org/tutorials/vector_api_tut.html.
//
// Vector support in godal is incomplete and should be considered a
// work in progress. The API may change in backwards incompatible ways.
func Example_vectorTutorial() {
	/*
		#include "gdal.h"

		int main() {
			GDALAllRegister();
	*/
	godal.RegisterAll()
	/*
		GDALDatasetH hDS;
		OGRLayerH hLayer;
		OGRFeatureH hFeature;
		OGRFeatureDefnH hFDefn;

		hDS = GDALOpenEx( "point.shp", GDAL_OF_VECTOR, NULL, NULL, NULL );
		if( hDS == NULL ) {
			printf( "Open failed.\n" );
			exit( 1 );
		}
	*/

	//by using the VectorOnly() option Open() will return an error if given
	//a raster dataset
	hDS, err := godal.Open("testdata/test.geojson", godal.VectorOnly())
	if err != nil {
		panic(err)
	}
	/*
		hLayer = GDALDatasetGetLayerByName( hDS, "point" );
		hFDefn = OGR_L_GetLayerDefn(hLayer);
		OGR_L_ResetReading(hLayer);
		while( (hFeature = OGR_L_GetNextFeature(hLayer)) != NULL ) {
	*/
	layers := hDS.Layers()
	for _, layer := range layers {
		layer.ResetReading()
		for {
			/*
				int iField;
				OGRGeometryH hGeometry;
				for( iField = 0; iField < OGR_FD_GetFieldCount(hFDefn); iField++ ) {
					OGRFieldDefnH hFieldDefn = OGR_FD_GetFieldDefn( hFDefn, iField );
					switch( OGR_Fld_GetType(hFieldDefn) ) {
					case OFTInteger:
						printf( "%d,", OGR_F_GetFieldAsInteger( hFeature, iField ) );
						break;
					case OFTInteger64:
						printf( CPL_FRMT_GIB ",", OGR_F_GetFieldAsInteger64( hFeature, iField ) );
						break;
					case OFTReal:
						printf( "%.3f,", OGR_F_GetFieldAsDouble( hFeature, iField) );
						break;
					case OFTString:
						printf( "%s,", OGR_F_GetFieldAsString( hFeature, iField) );
						break;
					default:
						printf( "%s,", OGR_F_GetFieldAsString( hFeature, iField) );
						break;
					}
				}
			*/
			feat := layer.NextFeature()
			if feat == nil {
				break
			}
			fields := feat.Fields()
			fmt.Printf("%v\n", fields)

			/*
				hGeometry = OGR_F_GetGeometryRef(hFeature);
				if( hGeometry != NULL
					&& wkbFlatten(OGR_G_GetGeometryType(hGeometry)) == wkbPoint )
					printf( "%.3f,%3.f\n", OGR_G_GetX(hGeometry, 0), OGR_G_GetY(hGeometry, 0) );
				else
					printf( "no point geometry\n" );
			*/
			geom := feat.Geometry()
			wkt, _ := geom.WKT()
			fmt.Printf("geom: %s\n", wkt)

			/*
					OGR_F_Destroy( hFeature );
				}
			*/
			//geom.Close is a no-op in this case. We call it nonetheless, as it is strongly recommended
			//to call Close on an object that implements the method to avoid potential memory leaks.
			geom.Close()

			//calling feat.Close is mandatory to prevent memory leaks
			feat.Close()
		}
	}
	/*
			GDALClose( hDS );
		}
	*/
	hDS.Close()

	/*
		const char *pszDriverName = "ESRI Shapefile";
		GDALDriverH hDriver;
		GDALDatasetH hDS;
		OGRLayerH hLayer;
		OGRFieldDefnH hFieldDefn;
		double x, y;
		char szName[33];

		GDALAllRegister();

		hDriver = GDALGetDriverByName( pszDriverName );
		if( hDriver == NULL )
		{
			printf( "%s driver not available.\n", pszDriverName );
			exit( 1 );
		}

		hDS = GDALCreate( hDriver, "point_out.shp", 0, 0, 0, GDT_Unknown, NULL );
		if( hDS == NULL )
		{
			printf( "Creation of output file failed.\n" );
			exit( 1 );
		}
	*/
	hDS, err = godal.CreateVector(godal.GeoJSON, "/vsimem/point_out.geojson")
	if err != nil {
		panic(err)
	}
	defer godal.VSIUnlink("/vsimem/point_out.geojson")

	/*
	   hLayer = GDALDatasetCreateLayer( hDS, "point_out", NULL, wkbPoint, NULL );
	   if( hLayer == NULL )
	   {
	       printf( "Layer creation failed.\n" );
	       exit( 1 );
	   }

	   hFieldDefn = OGR_Fld_Create( "Name", OFTString );

	   OGR_Fld_SetWidth( hFieldDefn, 32);

	   if( OGR_L_CreateField( hLayer, hFieldDefn, TRUE ) != OGRERR_NONE )
	   {
	       printf( "Creating Name field failed.\n" );
	       exit( 1 );
	   }

	   OGR_Fld_Destroy(hFieldDefn);
	*/

	layer, err := hDS.CreateLayer("point_out", nil, godal.GTPoint,
		godal.NewFieldDefinition("Name", godal.FTString))
	if err != nil {
		panic(fmt.Errorf("Layer creation failed: %w", err))
	}

	/*
	   while( !feof(stdin)
	       && fscanf( stdin, "%lf,%lf,%32s", &x, &y, szName ) == 3 )
	   {
	       OGRFeatureH hFeature;
	       OGRGeometryH hPt;

	       hFeature = OGR_F_Create( OGR_L_GetLayerDefn( hLayer ) );
	       OGR_F_SetFieldString( hFeature, OGR_F_GetFieldIndex(hFeature, "Name"), szName );

	       hPt = OGR_G_CreateGeometry(wkbPoint);
	       OGR_G_SetPoint_2D(hPt, 0, x, y);

	       OGR_F_SetGeometry( hFeature, hPt );
	       OGR_G_DestroyGeometry(hPt);

	       if( OGR_L_CreateFeature( hLayer, hFeature ) != OGRERR_NONE )
	       {
	       printf( "Failed to create feature in shapefile.\n" );
	       exit( 1 );
	       }

	       OGR_F_Destroy( hFeature );
	   }
	*/
	//scanner := bufio.NewScanner(os.Stdin)
	scanner := bufio.NewScanner(strings.NewReader(`POINT (1 1)`))
	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		geom, _ := godal.NewGeometryFromWKT(scanner.Text(), nil)
		feat, err := layer.NewFeature(geom)
		//godal does not yet support setting fields on newly created features
		if err != nil {
			panic(fmt.Errorf("Failed to create feature in shapefile: %w", err))
		}
		gj, _ := feat.Geometry().GeoJSON()
		fmt.Printf("created geometry %s\n", gj)

		feat.Close()
	}
	/*

	   GDALClose( hDS );
	*/
	err = hDS.Close() //Close must be called and the error must be checked when writing
	if err != nil {
		panic(fmt.Errorf("failed to close shapefile: %w", err))
	}

	// Output:
	// [{0 foo true 4 bar}]
	// geom: POLYGON ((100 0,101 0,101 1,100 1,100 0))
	// [{0 foo true 4 baz}]
	// geom: POLYGON ((100 0,101 0,101 1,100 1,100 0))
	// created geometry { "type": "Point", "coordinates": [ 1.0, 1.0 ] }
}

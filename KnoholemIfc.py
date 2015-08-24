import copy
import os
import sys
import subprocess
from decimal import Decimal
from urllib.request import ProxyHandler, build_opener, install_opener
from builtins import range

from rdflib import Graph, RDF, URIRef, Namespace, Literal
from SPARQLWrapper import SPARQLWrapper, JSON
# from KnoIfc.KnoholemVis import KnoholemVisual
from rdflib.resource import Resource

__author__ = 'Diarmuid Ryan'


class KnoholemIfc:
    HEIGHT_OF_WALLS = 2

    sparql_prefix = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX knoholem: <http://www.semanticweb.org/ontologies/2012/9/knoholem.owl#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>"""

    knoToIfcSensor = {"CO2Sensor": "CO2SENSOR", "EnergyMeter": "ENERGYMETER", "FireSensor": "FIRESENSOR",
                      "AirFlowSensor": "WINDSENSOR", "WaterFlowSensor": "FLOWSENSOR",
                      "HumiditySensor": "HUMIDITYSENSOR", "LuminanceSensor": "LIGHTSENSOR",
                      "OpeningSensor": "CONTACTSENSOR", "TemperatureSensor": "TEMPERATURESENSOR"}

    knoToIfcEntity = {"EnergyMeter": "IfcFlowMeter"}  # defaults to IfcSensor if none specified here

    ifc_ns = Namespace("http://www.buildingsmart-tech.org/ifcOWL#")
    cart_ns = Namespace("http://purl.org/net/cartCoord#")
    rdfs = Namespace("http://www.w3.org/2000/01/rdf-schema#")

    def __init__(self, sparql_endpoint_url, sparql_graph_name,
                 original_uri="http://www.semanticweb.org/ontologies/2012/9/knoholem.owl#",
                 uri_to_use="http://something/example/"):
        """
        Setup the KnoholemIfc class.
        :param sparql_endpoint_url: The URL of the sparql endpoint to be used for all sparql operations
        :param sparql_graph_name: The URL of the sparql graph to take input from. Output = sparql_graph_name + "_Ifc"
        :param original_uri: The uri of the dataset in the input graph
        :param uri_to_use: The uri to use for the output dataset
        """
        proxy = ProxyHandler({})
        opener = build_opener(proxy)
        install_opener(opener)

        self.sparql_endpoint = sparql_endpoint_url
        print("Loading IFC Ontology")
        print(sparql_endpoint_url)
        self.sparql = SPARQLWrapper(sparql_endpoint_url)
        self.sparql_graph = sparql_graph_name
        self.sparql_graph_uri = original_uri
        self.out_ns = Namespace(uri_to_use)
        self.out_graph = Graph(identifier=uri_to_use)
        self.out_graph.namespace_manager.bind("ifc", self.ifc_ns)
        self.out_graph.namespace_manager.bind("cart", self.cart_ns)
        self.out_graph.namespace_manager.bind("rdfs", self.rdfs)
        self.out_graph.namespace_manager.bind("", self.out_ns)
        output_sparql_graph_name = sparql_graph_name + "_Ifc"
        # self.visualize = KnoholemVisual(self.sparql_prefix, self.sparql, self.sparql_graph, output_sparql_graph_name)
        print("Starting conversion process")
        self.convert()
        # self.visualize.close()
        print("Writing converted data to file")
        output_filename = os.path.join("output", "knoholemifc.n3")
        output_file = open(output_filename, "wb")
        self.out_graph.serialize(destination=output_file, format='n3', auto_compact=True)
        output_file.close()
        print("Writing converted data to fuseki graph: " + output_sparql_graph_name)
        subprocess.call("ruby {2} {0} {1}".format(sparql_endpoint_url, output_sparql_graph_name,
                                                  os.path.join("fuseki", "s-delete")))
        print("Old graph deleted, adding new graph")
        subprocess.call("ruby {3} {0} {1} {2}".format(sparql_endpoint_url, output_sparql_graph_name,
                                                      output_filename, os.path.join("fuseki", "s-put")))
        print("Finished")

    def run_sparql_query(self, query) -> dict:
        """
            Simply runs a sparql query
            :rtype : dict
            :param query: The query to be run
            :return: Returns a dict containing the return of the sparql query
            """
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)
        return self.sparql.query().convert()

    def strip_uri(self, to_be_stripped, url=None) -> str:
        """
            Takes the namespace off an Individual
            :rtype : str
            :param to_be_stripped: The full URI of the individual to be stripped
            :param url: the namespace of the individual. Leave empty to use the sparql_graph_uri from the constructor
            :return: Returns the stripped URI
            """
        if url is None:
            url = self.sparql_graph_uri
        if url in to_be_stripped:
            return to_be_stripped[len(url):]
        else:
            print("ERROR, {1:s} is not in {0:s}".format(to_be_stripped, url))

    def convert(self):
        """
            Starts the conversion process of the Knoholem Graph to IfcOWL
        """
        query = """
            {0:s}
            SELECT ?y ?perim ?name
            FROM <{1:s}>
            WHERE {{
                ?y rdf:type knoholem:Room .
                ?y knoholem:hasPerimeter ?perim .
                ?y knoholem:hasName ?name
            }}""".format(self.sparql_prefix, self.sparql_graph)
        results = self.run_sparql_query(query)
        for result in results["results"]["bindings"]:
            qualified_room_name = result["y"]["value"]
            room_name = self.strip_uri(qualified_room_name)
            room = self.out_graph.resource(self.out_ns + room_name)
            self._add_room_placement_cart_coord(room, str(result["perim"]["value"]), room_name)
            room.set(RDF.type, URIRef(self.ifc_ns.IfcSpace))
            room_label = Literal(result["name"]["value"])
            room.set(self.rdfs.label, room_label)
            # self.visualize.put_room(qualified_room_name, str(room.identifier))
            contained_in_room = self.out_graph.resource(self.out_ns + "Contained_In_" + room_name)
            contained_in_room.set(RDF.type, URIRef(self.ifc_ns.IfcRelContainedInSpatialStructure))
            contained_in_room.set(self.ifc_ns.RelatingStructure_of_IfcRelContainedInSpatialStructure, room)
            self._add_room_placement_ifc_full(room, str(result["perim"]["value"]), room_name, contained_in_room)
            self.convert_sensors(contained_in_room, qualified_room_name)

    def _add_room_placement_cart_coord(self, place: Resource, perimeter: str, place_name: str):
        """
        This method adds the coordinates of an individual given a string of the perimeter coordinates
        It outputs them in cartCoord syntax
        :type place_name: str
        :type perimeter: str
        :type place: Resource
        :param place: A resource for which the coordinates are to be assigned
        :param perimeter: A string of coordinates in the form x1:y1;x2:y2;...
        :param place_name: The name of the resource for which the coordinates are to be assigned
        :return: None
        """
        point_list = self.out_graph.resource(self.out_ns + "coords_of_" + place_name)
        point_list.set(RDF.type, URIRef(self.cart_ns.Point_List))
        place.set(self.cart_ns.hasPlacement, point_list)
        index = perimeter.find(';')
        loop_count = 0
        while index != -1:
            point_string = perimeter[0:index]
            perimeter = perimeter[index + 1:]
            point = self.out_graph.resource(self.out_ns + place_name + "_point_" + str(loop_count))
            point.set(RDF.type, URIRef(self.cart_ns.Point))
            loop_count += 1
            point_list.add(self.cart_ns.hasPoint, point)
            colon = point_string.find(':')
            if colon is not -1:
                coord = Literal(point_string[0:colon], datatype="http://www.w3.org/2001/XMLSchema#double")
                point.set(self.cart_ns.xcoord, coord)
                point_string = point_string[colon + 1:]
                colon = point_string.find(':')
                if colon is not -1:
                    coord = Literal(point_string[0:colon], datatype="http://www.w3.org/2001/XMLSchema#double")
                else:
                    coord = Literal(point_string, datatype="http://www.w3.org/2001/XMLSchema#double")
                point.set(self.cart_ns.ycoord, coord)
                point_string = point_string[colon + 1:]
                colon = point_string.find(':')
                if colon is not -1:
                    coord = Literal(point_string[0:colon], datatype="http://www.w3.org/2001/XMLSchema#double")
                    point.set(self.cart_ns.zcoord, coord)
            index = perimeter.find(';')

    def _add_room_placement_ifc_full(self, room, perimeter, room_name, contained_in_room):
        """
        This method add's the coordinates of an ifcSpace given a string of the perimiter coordinates
        It outputs them in the full Ifc syntax
        :type room_name: str
        :type perimeter: str
        :type room: Resource
        :param room: A resource for the IfcSpace to be added
        :param perimeter: The string of perimiter coordinates in the form x:y;x:y;...
        :param room_name: A string of the name of the IfcSpace
        """

        def add_corner(coord, counter):
            point_list = self.out_graph.resource(
                self.out_ns + room_name + "_boundary_" + str(index) + "_points_" + str(counter))
            point_list.set(RDF.type, URIRef(self.ifc_ns.IfcCartesianPoint_List))
            line.add(self.ifc_ns.Points, point_list)
            point = self.out_graph.resource(
                self.out_ns + room_name + "_boundary_" + str(index) + "_point_" + str(counter))
            point.set(RDF.type, URIRef(self.ifc_ns.IfcCartesianPoint))
            xcoord = create_coord_list(coord["x"],
                                       room_name + "_boundary_" + str(index) + "_point_" + str(counter) + "_x")
            point.set(self.ifc_ns.Coordinates, xcoord)
            ycoord = create_coord_list(coord["y"],
                                       room_name + "_boundary_" + str(index) + "_point_" + str(counter) + "_y")
            xcoord.set(self.ifc_ns.hasNext, ycoord)
            zcoord = create_coord_list(coord["z"],
                                       room_name + "_boundary_" + str(index) + "_point_" + str(counter) + "_z")
            ycoord.set(self.ifc_ns.hasNext, zcoord)
            point_list.add(self.ifc_ns.hasListContent, point)
            return point_list

        def add_face(coord1, coord2):
            coord3 = copy.deepcopy(coord2)
            coord4 = copy.deepcopy(coord1)
            coord3["z"] = self.HEIGHT_OF_WALLS
            coord4["z"] = self.HEIGHT_OF_WALLS
            point_list1 = add_corner(coord1, 0)
            point_list2 = add_corner(coord2, 1)
            point_list1.set(self.ifc_ns.hasNext, point_list2)
            point_list3 = add_corner(coord3, 2)
            point_list2.set(self.ifc_ns.hasNext, point_list3)
            point_list4 = add_corner(coord4, 3)
            point_list3.set(self.ifc_ns.hasNext, point_list4)
            point_list4.set(self.ifc_ns.hasNext, point_list1)

        def create_coord_list(coord_val, name_append):
            coord_list = self.out_graph.resource(self.out_ns + name_append)
            coord_list.set(RDF.type, URIRef(self.ifc_ns.IfcLengthMeasure_List))
            coord = Literal(coord_val, datatype=URIRef(self.ifc_ns.IfcLengthMeasure))
            coord_list.set(self.ifc_ns.hasListContent, coord)
            return coord_list

        def coord_string_to_array(coords_string):
            """
            :type coords_string: A string containing a list of the coords in the form x1:y1;x2:y2;...xN:yN;
            :rtype : list
            """
            coords_out = []
            semi_col_pos = coords_string.find(';')
            while semi_col_pos != -1:
                coordinate = coords_string[0:semi_col_pos]
                coords_string = coords_string[semi_col_pos + 1:]
                semi_col_pos = coords_string.find(';')
                colon_pos = coordinate.find(':')
                coords_out.append({
                    "x": Decimal(coordinate[0:colon_pos]),
                    "y": Decimal(coordinate[colon_pos + 1:]),
                    "z": 0
                })
            return coords_out

        overall_boundary = self.out_graph.resource(self.out_ns + room_name + "_boundary")
        overall_boundary.set(RDF.type, URIRef(self.ifc_ns.IfcRelSpaceBoundary2ndLevel))
        overall_boundary.set(self.ifc_ns.RelatingSpace, room)
        coords = coord_string_to_array(perimeter)
        coords.append(coords[0])
        for index in range(len(coords) - 1):
            wall = self.out_graph.resource(self.out_ns + room_name + "_wall_" + str(index))
            wall.set(RDF.type, URIRef(self.ifc_ns.IfcWallStandardCase))
            contained_in_room.add(self.ifc_ns.RelatedElements_of_IfcRelContainedInSpatialStructure, wall)
            sub_boundary = self.out_graph.resource(self.out_ns + room_name + "_boundary_" + str(index))
            sub_boundary.set(RDF.type, URIRef(self.ifc_ns.IfcRelSpaceBoundary2ndLevel))
            sub_boundary.set(self.ifc_ns.RelatedBuildingElement, wall)
            overall_boundary.add(self.ifc_ns.InnerBoundaries, sub_boundary)
            sub_boundary.add(self.ifc_ns.ParentBoundary, overall_boundary)
            csg = self.out_graph.resource(self.out_ns + room_name + "_boundary_" + str(index) + "_csg")
            csg.set(RDF.type, URIRef(self.ifc_ns.IfcConnectionSurfaceGeometry))
            sub_boundary.set(self.ifc_ns.ConnectionGeometry, csg)
            cbp = self.out_graph.resource(self.out_ns + room_name + "_boundary_" + str(index) + "_cbp")
            cbp.set(RDF.type, URIRef(self.ifc_ns.IfcCurveBoundedPlane))
            csg.set(self.ifc_ns.SurfaceOnRelatingElement, cbp)
            line = self.out_graph.resource(self.out_ns + room_name + "_boundary_" + str(index) + "_line")
            line.set(RDF.type, URIRef(self.ifc_ns.IfcPolyline))
            cbp.set(self.ifc_ns.OuterBoundary, line)
            add_face(coords[index], coords[index + 1])
            index += 1

    # def _add_placement_ifc_full(self, room: Resource, perimeter: str, room_name: str):
    #     """
    #     This method add's the coordinates of an ifcSpace given a string of the perimiter coordinates
    #     It outputs them in the full Ifc syntax
    #     :type room_name: str
    #     :type perimeter: str
    #     :type room: Resource
    #     :param room: A resource for the IfcSpace to be added
    #     :param perimeter: The string of perimiter coordinates in the form x:y;x:y;...
    #     :param room_name: A string of the name of the IfcSpace
    #     """
    #     # Create the IfcProductDefinitionShape and link the IfcSpace to it
    #     product_definition_shape = self.out_graph.resource(self.out_ns + "DefShape_" + room_name)
    #     product_definition_shape.set(RDF.type, URIRef(self.ifc_ns.IfcProductDefinitionShape))
    #     room.set(self.ifc_ns.Representation, product_definition_shape)
    #     # Create the IfcRepresentation_List and link the IfcProductDefinitionShape to it
    #     representation_list = self.out_graph.resource(self.out_ns + "Rep_list_" + room_name)
    #     representation_list.set(RDF.type, URIRef(self.ifc_ns.IfcRepresentation_List))
    #     representation_list.set(self.ifc_ns.hasNext, RDF.nil)
    #     product_definition_shape.set(self.ifc_ns.Representations, representation_list)
    #     # Create the IfcShapeRepresentation and put it in the IfcRepresentation_List
    #     shape_representation = self.out_graph.resource(self.out_ns + "RepShape_" + room_name)
    #     shape_representation.set(RDF.type, URIRef(self.ifc_ns.IfcShapeRepresentation))
    #     representation_list.set(self.ifc_ns.hasListContent, shape_representation)
    #     # Create the IfcFace and link the IfcShapeRepresentation to it
    #     face = self.out_graph.resource(self.out_ns + "face_" + room_name)
    #     face.set(RDF.type, URIRef(self.ifc_ns.IfcFace))
    #     shape_representation.set(self.ifc_ns.Items, face)
    #     # Create the IfcFaceBound and link the IfcFace to it
    #     face_bound = self.out_graph.resource(self.out_ns + "face_bound_" + room_name)
    #     face_bound.set(RDF.type, URIRef(self.ifc_ns.IfcFaceBound))
    #     face.set(self.ifc_ns.Bounds, face_bound)
    #     # Create the IfcPolyLoop and link the IfcFaceBound to it
    #     poly_loop = self.out_graph.resource(self.out_ns + "poly_loop_" + room_name)
    #     poly_loop.set(RDF.type, URIRef(self.ifc_ns.IfcPolyLoop))
    #     face_bound.set(self.ifc_ns.Bound, poly_loop)
    #     # Create and add the IfcCartesianPoints and link the IfcPolyLoop to them
    #     loop_counter = 0
    #     other_counter = 0
    #     cart_point_list = self.out_graph.resource(
    #         self.out_ns + "cart_point_list_" + room_name + "_" + str(loop_counter))
    #     cart_point_list.set(RDF.type, URIRef(self.ifc_ns.IfcCartesianPoint_List))
    #     poly_loop.set(self.ifc_ns.Polygon, cart_point_list)
    #     index = perimeter.find(';')
    #     while index != -1:
    #         one_coord = perimeter[0:index]
    #         perimeter = perimeter[index + 1:]
    #         colon = one_coord.find(':')
    #         xcoord = Decimal(one_coord[0:colon])
    #         ycoord = Decimal(one_coord[colon + 1:])
    #         cartesian_point = self.out_graph.resource(self.out_ns + "cart_point_" + room_name + "_" + str(loop_counter))
    #         cartesian_point.set(RDF.type, URIRef(self.ifc_ns.IfcCartesianPoint))
    #         cart_point_list.add(self.ifc_ns.hasListContent, cartesian_point)
    #         loop_counter += 1
    #         coords_list_x = self.out_graph.resource(self.out_ns + "coord_list_" + room_name + "_" + str(other_counter))
    #         coords_list_x.set(RDF.type, URIRef(self.ifc_ns.IfcLengthMeasureList))
    #         cartesian_point.add(self.ifc_ns.Coordinates_of_IfcCartesianPoint, coords_list_x)
    #         other_counter += 1
    #         xcoord_measure = Literal(xcoord, datatype=URIRef(self.ifc_ns.IfcLengthMeasure))
    #         coords_list_x.add(self.ifc_ns.hasListContent, xcoord_measure)
    #         coords_list_y = self.out_graph.resource(self.out_ns + "coord_list_" + room_name + "_" + str(other_counter))
    #         coords_list_y.set(RDF.type, URIRef(self.ifc_ns.IfcLengthMeasureList))
    #         coords_list_x.add(self.ifc_ns.hasNext, coords_list_y)
    #         other_counter += 1
    #         ycoord_measure = Literal(ycoord, datatype=URIRef(self.ifc_ns.IfcLengthMeasure))
    #         coords_list_y.add(self.ifc_ns.hasListContent, ycoord_measure)
    #         coords_list_y.add(self.ifc_ns.hasNext, RDF.nil)
    #         index = perimeter.find(';')
    #         if index is not -1:
    #             cart_point_list_new = self.out_graph.resource(
    #                 self.out_ns + "cart_point_list_" + room_name + "_" + str(loop_counter))
    #             cart_point_list_new.set(RDF.type, URIRef(self.ifc_ns.IfcCartesianPoint_List))
    #             cart_point_list.set(self.ifc_ns.hasNext, cart_point_list_new)
    #             cart_point_list = cart_point_list_new
    #         else:
    #             cart_point_list.set(self.ifc_ns.hasNext, RDF.nil)

    def convert_sensors(self, contained_in_room, qualified_room_name):
        """
        Converts the sensors of a Knoholem Dataset to IfcOWL
        :type qualified_room_name: str
        :type contained_in_room: Resource
        :param contained_in_room: A resource of type ifcRelContainedInStructure
        :param qualified_room_name: The name of the relating structure to contained_in_room
        :return: None
        """
        query = """%s
            SELECT ?y
            FROM <%s>
            WHERE {
                ?y knoholem:isSensorOf %s
            }""" % (self.sparql_prefix, self.sparql_graph, "<" + qualified_room_name + ">")
        sensor_results = self.run_sparql_query(query)

        def get_type_for_sensor(result_set):
            index = 0
            result = result_set["results"]["bindings"][index]["type"]["value"]
            while result.find("http://www.w3.org/2002/07/owl#NamedIndividual") is not -1:
                index += 1
                result = result_set["results"]["bindings"][index]["type"]["value"]
            return self.strip_uri(result)

        for each in sensor_results["results"]["bindings"]:
            sensor_name_uri = each["y"]["value"]
            # self.visualize.put_sensor(sensor_name_uri)
            sensor_name = self.strip_uri(sensor_name_uri)
            query = u"""{0:s}
                SELECT ?type ?x ?y ?name
                FROM <{1:s}>
                WHERE {{
                    <{2:s}> rdf:type ?type .
                    <{2:s}> knoholem:hasName ?name .
                    <{2:s}> knoholem:hasPlacement ?pos .
                    ?pos knoholem:hasXCoord ?x .
                    ?pos knoholem:hasYCoord ?y
                }}""".format(self.sparql_prefix, self.sparql_graph, sensor_name_uri)
            this_sensor_result = self.run_sparql_query(query)
            this_sensor_type_kno = get_type_for_sensor(this_sensor_result)
            if this_sensor_type_kno in self.knoToIfcSensor:
                sensor_type_ifc = self.knoToIfcSensor[this_sensor_type_kno]
            else:
                sensor_type_ifc = "UNDEFINED"
            if this_sensor_type_kno in self.knoToIfcEntity:
                sensor_entity_ifc = self.knoToIfcEntity[this_sensor_type_kno]
            else:
                sensor_entity_ifc = "IfcSensor"
            sensor = self.out_graph.resource(self.out_ns + sensor_name)
            sensor.set(RDF.type, URIRef(self.ifc_ns + sensor_entity_ifc))
            sensor_label = Literal(this_sensor_result["results"]["bindings"][0]["name"]["value"])
            sensor.set(self.rdfs.label, sensor_label)
            if sensor_entity_ifc is "IfcSensor":
                sensor.set(self.ifc_ns.PredefinedType_of_IfcSensor, URIRef(self.ifc_ns + sensor_type_ifc))
            else:
                sensor.set(self.ifc_ns.PredefinedType_of_IfcFlowMeter, URIRef(self.ifc_ns + sensor_type_ifc))
            contained_in_room.add(self.ifc_ns.RelatedElements_of_IfcRelContainedInSpatialStructure, sensor)
            sensor_point = self.out_graph.resource(self.out_ns + sensor_name + "_point")
            sensor_point.set(RDF.type, self.cart_ns.Point)
            xcoord = Literal(this_sensor_result["results"]["bindings"][0]["x"]["value"],
                             datatype="http://www.w3.org/2001/XMLSchema#double")
            ycoord = Literal(this_sensor_result["results"]["bindings"][0]["y"]["value"],
                             datatype="http://www.w3.org/2001/XMLSchema#double")
            sensor_point.set(self.cart_ns.xcoord, xcoord)
            sensor_point.set(self.cart_ns.ycoord, ycoord)
            sensor.set(self.cart_ns.hasPlacement, sensor_point)


if __name__ == "__main__":
    # rdf_stf("http://localhost:3030/ifcowl/", "http://localhost:3030/ifcowl/data/knoholem.owl")
    KnoholemIfc(sys.argv[1], sys.argv[2])

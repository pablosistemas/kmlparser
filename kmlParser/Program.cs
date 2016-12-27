using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Spatial;
using System.Xml;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Diagnostics;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Types;

/* Programa atualiza documentos num banco MongoDB complementando informação de posicionamento 
 com cidade, estado, sigla, micro e mesorregiões. Dados buscados de arquivo KML cidades Brasil.
 Fonte dos dados: http://www.gmapas.com/poligonos-ibge/municipios-do-brasil */

namespace kmlParser
{

    public class GeographicCoordinate
    {
        private const double Tolerance = 0.0001;

        public GeographicCoordinate(double longitude, double latitude)
        {
            this.Longitude = longitude;
            this.Latitude = latitude;
        }

        public double Latitude { get; set; }
        public double Longitude { get; set; }
        
        public static bool operator ==(GeographicCoordinate a, GeographicCoordinate b)
        {
            // If both are null, or both are same instance, return true.
            if (ReferenceEquals(a, b))
            {
                return true;
            }

            // If one is null, but not both, return false.
            if (((object)a == null) || ((object)b == null))
            {
                return false;
            }

            var latResult = Math.Abs(a.Latitude - b.Latitude);
            var lonResult = Math.Abs(a.Longitude - b.Longitude);
            return (latResult < Tolerance) && (lonResult < Tolerance);
        }

        public static bool operator !=(GeographicCoordinate a, GeographicCoordinate b)
        {
            return !(a == b);
        }

        public override bool Equals(object obj)
        {
            // Check for null values and compare run-time types.
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            var p = (GeographicCoordinate)obj;
            var latResult = Math.Abs(this.Latitude - p.Latitude);
            var lonResult = Math.Abs(this.Longitude - p.Longitude);
            return (latResult < Tolerance) && (lonResult < Tolerance);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (this.Latitude.GetHashCode() * 397) ^ this.Longitude.GetHashCode();
            }
        }
    }

    public class Brazil
    {
        public static string GetState(string state)
        {
            switch (state)
            {
                case "AC":
                    return "ACRE";

                case "AL":
                    return "ALAGOAS";

                case "AP":
                    return "AMAPÁ";

                case "AM":
                    return "AMAZONAS";

                case "BA":
                    return "BAHIA";

                case "CE":
                    return "CEARA";

                case "DF":
                    return "DISTRITO FEDERAL";

                case "ES":
                    return "ESPIRITO SANTO";

                case "GO":
                    return "GOAIS";

                case "MA":
                    return "MARANHAO";

                case "MT":
                    return "MATO GROSSO";

                case "MS":
                    return "MATO GROSSO DO SUL";

                case "MG":
                    return "MINAS GERAIS";

                case "PA":
                    return "PARA";

                case "PB":
                    return "PARAIBA";

                case "PR":
                    return "PARANA";

                case "PE":
                    return "PERNAMBUCO";

                case "PI":
                    return "PIAUI";

                case "RJ":
                    return "RIO DE JANEIRO";

                case "RN":
                    return "RIO GRANDE DO NORTE";

                case "RS":
                    return "RIO GRANDE DO SUL";

                case "RO":
                    return "RONDONIA";

                case "RR":
                    return "RORAIMA";

                case "SC":
                    return "SANTA CATARINA";

                case "SP":
                    return "SAO PAULO";

                case "SE":
                    return "SERGIPE";

                case "TO":
                    return "TOCANTINS";
            }

            throw new Exception("Not Available");
        }
    }

    class DbGeographyUtil
    {

        Dictionary<String, DbGeography> cityCoordinatesList = new Dictionary<String, DbGeography>();
        public Dictionary<String, DbGeography> Coordenadas
        {
            get
            {
                return cityCoordinatesList;
            }
        }

        public string formatCityKeyFormatted(string cityName, string estado, string sigla, string mesoregiao, string nomemeso, string microregiao, string nomemicro)
        {
            StringBuilder key = new StringBuilder();
            key.Append(cityName + ",");
            key.Append(estado + ",");
            key.Append(sigla + ",");
            key.Append(mesoregiao + ",");
            key.Append(nomemeso + ",");
            key.Append(microregiao + ",");
            key.Append(nomemicro);
            return key.ToString();
        }

        public Task<string> buscaCidadePorLatLongAsync(DbGeography coord)
        {
            return Task.Factory.StartNew(() =>
            {
                foreach (KeyValuePair<string, DbGeography> city in cityCoordinatesList)
                {
                    //if (coord.Distance(city.Value) <= 0)
                    if (coord.Intersects(city.Value))
                    {
                        return city.Key;
                    }
                }
                return null;
            });
        }

        public string buscaCidadePorLatLong(DbGeography coord)
        {
            foreach(KeyValuePair<string, DbGeography> city in cityCoordinatesList)
            {
                //DbGeography novoPonto = DbGeography.FromText(@"POINT(" + coord.Latitude + " " + coord.Longitude + ")");

                if (coord.Intersects(city.Value))
                {
                    return city.Key;
                }
            }
            return "";
        }

        public static DbGeography ConvertLatLonToDbGeography(double longitude, double latitude)
        {
            var point = string.Format("POINT({1} {0})", latitude, longitude);
            return DbGeography.FromText(point);
        }

        public static GeographicCoordinate ConvertStringArrayToGeographicCoordinates(string pointString)
        {
            var points = pointString.Split(',');
            var coordinates = new GeographicCoordinate(double.Parse(points[0]), double.Parse(points[1]));

            return coordinates;
        }

        public static DbGeography ConvertGeoCoordinatesToPolygon(IEnumerable<GeographicCoordinate> coordinates)
        {
            var coordinateList = coordinates.ToList();
            if (coordinateList.First() != coordinateList.Last())
            {
                throw new Exception("First and last point do not match. This is not a valid polygon");
            }

            var count = 0;
            var sb = new StringBuilder();
            sb.Append(@"POLYGON((");
            foreach (var coordinate in coordinateList)
            {
                if (count == 0)
                {
                    sb.Append(coordinate.Longitude + " " + coordinate.Latitude);
                }
                else
                {
                    sb.Append("," + coordinate.Longitude + " " + coordinate.Latitude);
                }

                count++;
            }

            sb.Append(@"))");

            DbGeography returnCoordinate = DbGeography.FromText(sb.ToString(), DbGeography.DefaultCoordinateSystemId);
            returnCoordinate = DbGeography.FromText(SqlGeometry.STGeomFromWKB(new SqlBytes(returnCoordinate.AsBinary()),
                DbGeography.DefaultCoordinateSystemId).MakeValid().STUnion(SqlGeometry.STGeomFromWKB(new SqlBytes(returnCoordinate.StartPoint.AsBinary()),
                DbGeography.DefaultCoordinateSystemId)).STAsText().ToSqlString().ToString(), DbGeography.DefaultCoordinateSystemId);

            return returnCoordinate;

            // coordinateSystemId: 4326
            //return DbGeography.PolygonFromText(sb.ToString(), 4326);
        }

        public void Fence_ImportBrazilianCitiesFromKMLFile()
        {
            int importCounter = 0;
            StringBuilder errorMessages = new StringBuilder();

            using (StreamReader fileReader = new StreamReader("C:\\Users\\Pablo\\Downloads\\brasil.kml"))
            {
                XmlDocument xml = new XmlDocument();
                xml.Load(fileReader);

                using (XmlNodeList placemarkNodeList = xml.GetElementsByTagName("Placemark"))
                {
                    foreach (XmlNode placemark in placemarkNodeList)
                    {
                        string cityName = null;
                        string cityCode = null;
                        string stateName = null;
                        string stateCode = null;
                        string mesoregiao = null;
                        string nomemeso = null;
                        string microregiao = null;
                        string nomemicro = null;

                        try
                        {
                            XmlElement nameElement = placemark["name"];
                            if (nameElement != null)
                            {
                                cityName = nameElement.InnerText;
                            }

                            XmlElement extendedDataElement = placemark["ExtendedData"];
                            if (extendedDataElement != null)
                            {
                                XmlNodeList simpleDataNodeList = extendedDataElement.GetElementsByTagName("SimpleData");
                                foreach (XmlNode simpleData in simpleDataNodeList)
                                {
                                    if (simpleData.Attributes.Count > 0 && simpleData.Attributes["name"] != null)
                                    {
                                        XmlAttribute nameAttribute = simpleData.Attributes["name"];
                                        if (nameAttribute.Value == "GEOCODIG_M")
                                        {
                                            cityCode = simpleData.InnerText;
                                        }
                                        else if (nameAttribute.Value == "SIGLA")
                                        {
                                            stateCode = simpleData.InnerText;
                                            // pega nome do estado pela sigla
                                            stateName = Brazil.GetState(stateCode);
                                        }
                                        else if (nameAttribute.Value == "MESORREGIã")
                                        {
                                            mesoregiao = simpleData.InnerText;
                                        }
                                        else if (nameAttribute.Value == "NOME_MESO")
                                        {
                                            nomemeso = simpleData.InnerText;
                                        }
                                        else if (nameAttribute.Value == "MICRORREGI")
                                        {
                                            microregiao = simpleData.InnerText;
                                        }
                                        else if (nameAttribute.Value == "NOME_MICRO")
                                        {
                                            nomemicro = simpleData.InnerText;
                                        }
                                    }
                                }
                            }

                            // Placemark pode ter polygon ou multiGeometry->polygon
                            XmlElement polygonElement = placemark["Polygon"];
                            if (polygonElement != null)
                            {
                                DbGeography coordinates = GetCoordinatesFromKMLPolygonElement(polygonElement);
                                if (coordinates != null && stateName != null && stateCode != null && mesoregiao != null && 
                                        nomemeso != null && microregiao != null && nomemicro != null)
                                {
                                    //cityCoordinatesList.Add(coordinates);
                                    if (cityName != null)
                                        cityCoordinatesList[formatCityKeyFormatted(cityName, stateName, stateCode, mesoregiao, nomemeso, microregiao, nomemicro)] = coordinates;
                                }
                            }

                            XmlElement multiGeometryElement = placemark["MultiGeometry"];
                            if (multiGeometryElement != null)
                            {
                                XmlNodeList polygonNodeList = multiGeometryElement.GetElementsByTagName("Polygon");
                                foreach (XmlNode polygon in polygonNodeList)
                                {
                                    DbGeography coordinates = GetCoordinatesFromKMLPolygonElement((XmlElement)polygon);
                                    if (coordinates != null)
                                    {
                                        //cityCoordinatesList.Add(coordinates);
                                        if(cityName != null)
                                            cityCoordinatesList[cityName] = coordinates;
                                    }
                                }
                            }

                            if (!String.IsNullOrWhiteSpace(cityName) && !String.IsNullOrWhiteSpace(cityCode) && !String.IsNullOrWhiteSpace(stateName) &&
                                !String.IsNullOrWhiteSpace(stateCode) && cityCoordinatesList.Count > 0)
                            {
                                /*CreateOrUpdateFencesForCitiesImport(controller, Brazil.CountryCode, Brazil.CountryName, stateCode, stateName, cityCode, cityName,
                                    cityCoordinatesList);*/

                                ++importCounter;
                                //Console.WriteLine("Cidade {0} importada", importCounter);
                            }
                        }
                        catch (Exception ex)
                        {
                            errorMessages.AppendLine(!String.IsNullOrWhiteSpace(cityName) ? String.Format("{0} - {1}", cityName, ex.Message) : ex.Message);
                        }
                    }
                }
            }


            if (!String.IsNullOrWhiteSpace(errorMessages.ToString()))
            {
                throw new Exception(errorMessages.ToString());
            }

            //Assert.IsTrue(importCounter == 5564);
            //Assert.IsTrue(String.IsNullOrWhiteSpace(errorMessages.ToString()));
        }

        // CoordinateOrder.LongitudeLatitude
        public DbGeography GetCoordinateFromFormattedString(IEnumerable<string> pontos)
        {
            GeographicCoordinate geographyObj;
            List<GeographicCoordinate> listOfGeographicCoordinate = new List<GeographicCoordinate>();

            foreach (string ponto in pontos)
            {
                geographyObj = ConvertStringArrayToGeographicCoordinates(ponto);
                listOfGeographicCoordinate.Add(geographyObj);
            }

            DbGeography polygon = ConvertGeoCoordinatesToPolygon(listOfGeographicCoordinate);
            return polygon;
        }

        private DbGeography GetCoordinatesFromKMLPolygonElement(XmlElement polygonElement)
        {
            DbGeography coordinates = null;
            if (polygonElement != null)
            {
                XmlNodeList coordinatesNodeList = polygonElement.GetElementsByTagName("coordinates");
                if (coordinatesNodeList.Count > 0)
                {
                    string coordinatePoints = coordinatesNodeList[0].InnerText;
                    if (!String.IsNullOrWhiteSpace(coordinatePoints))
                    {
                        // transforma string de pontos separados por espaco em coordenadas do poligono (objeto DbGeography)
                        // haverá uma lista de strings contendo long,lat,grau
                        IEnumerable<string> coordinatePointList = coordinatePoints.Trim().Split(' ').Where(point => !String.IsNullOrWhiteSpace(point));
                        // O numero minimo de pontos para formar um poligono eh 4, sendo que o ultimo ponto deve sempre ser igual ao primeiro
                        if (coordinatePointList.Count() > 3)
                        {
                            coordinates = GetCoordinateFromFormattedString(coordinatePointList);
                        }
                    }
                }
            }
            return coordinates;
        }
    }

    class Program
    {
        public static void resolveDocumentos(IMongoCollection<BsonDocument> carroConectado, DbGeographyUtil objUtil, BsonDocument filter)
        {
            carroConectado.Find(filter).ForEachAsync(async (document) =>
            {
                try
                {
                    // teste sanidade
                    //string teste = await objUtil.buscaCidadePorLatLongAsync(DbGeography.FromText(new StringBuilder(@"POINT(-34.9427721 -8.151414)").ToString()));

                    BsonDocument data = (BsonDocument)document["Data"];
                    BsonDocument pos = (BsonDocument)data["Position"];
                    BsonArray point = (BsonArray)pos["Point"];

                    // well-know text format: https://en.wikipedia.org/wiki/Well-known_text
                    var sb = new StringBuilder();
                    sb.Append(@"POINT(");
                    sb.Append(point[0] + " " + point[1]);
                    sb.Append(@")");

                    // faz com que o controle volte para a funcao chamadora (.ForEachAsync) quando esse método roda
                    string completeCityName = await objUtil.buscaCidadePorLatLongAsync(DbGeography.FromText(sb.ToString()));

                    if (completeCityName == null)
                    {
                        return;
                    }

                    string[] rearrangedCityName = completeCityName.Split(',');
                    pos["Cidade"] = rearrangedCityName[0];
                    pos["Sigla"]  = rearrangedCityName[1];
                    pos["Estado"] = rearrangedCityName[2];
                    pos["MesoRegiao"]  = rearrangedCityName[3];
                    pos["NomeMeso"]    = rearrangedCityName[4];
                    pos["MicroRegiao"] = rearrangedCityName[5];
                    pos["NomeMicro"]   = rearrangedCityName[6];

                    // Atualiza "Data.Position" com cidade
                    var result = await carroConectado.FindOneAndUpdateAsync(
                            Builders<BsonDocument>.Filter.Eq("_id", document["_id"]),
                            Builders<BsonDocument>.Update.Set("Data", document["Data"])
                    );

                    Console.WriteLine("cityName {2} {1} {0}", point[0], point[1], pos["Cidade"]);
                }
                catch (Exception e)
                {
                    BsonDocument data = (BsonDocument)document["Data"];
                    BsonDocument pos = (BsonDocument)data["Position"];
                    BsonArray point = (BsonArray)pos["Point"];

                    Console.WriteLine("Erro: {1} {0}", point[0], point[1]);
                }

            }).Wait();
        }

        static void Main(string[] args)
        {
            var mongoClient = new MongoClient("mongodb://localhost:27017");
            var database = mongoClient.GetDatabase("local");

            IMongoCollection<BsonDocument> carroConectado = database.GetCollection<BsonDocument>("carroconectado3");
            
            DbGeographyUtil objUtil = new DbGeographyUtil();

            objUtil.Fence_ImportBrazilianCitiesFromKMLFile();

            // FIXME: trata objetos que nao foram atualizados na base de dados
            // Coordenadas nao sao "matched" com as coordenadas do KML fornecido
            var filter = new BsonDocument("Data.Position.Cidade", new BsonDocument("$exists", false));

            //var filter = new BsonDocument();

            resolveDocumentos(carroConectado, objUtil, filter);
        }
    }
}
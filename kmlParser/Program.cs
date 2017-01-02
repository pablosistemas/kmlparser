using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Spatial;
using System.Xml;
using System.IO;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Types;

/// <summary>
/// Programa atualiza documentos num banco MongoDB complementando os dados de posicionamento 
/// do carro com informações de cidade, estado, sigla, micro e mesorregiões. 
/// Os dados são buscados de arquivo KML das cidades  do Brasil.
/// Fonte dos dados: http://www.gmapas.com/poligonos-ibge/municipios-do-brasil */
/// </summary>

namespace kmlParser
{
    /// <summary>
    /// GeographicCoordinate: classe provê métodos para configurar, editar e validar informações
    /// de posicionamento em latitude e longitude
    /// </summary>

    public class GeographicCoordinate
    {
        private const double Tolerance = 0.0001;

        /// <summary>
        /// Construtor
        /// </summary>
        /// <param name="longitude">Auto</param>
        /// <param name="latitude">Auto</param>
        public GeographicCoordinate(double longitude, double latitude)
        {
            this.Longitude = longitude;
            this.Latitude = latitude;
        }

        public double Latitude { get; set; }
        public double Longitude { get; set; }

        /// <summary>
        /// Sobrecarga de operador de igualdade. Testa se dois objetos GeographicCoordinate
        /// possuem informações do mesmo local em latitude e longitude dentro de tolerância 
        /// especificada.
        /// </summary>
        /// <param name="a"> Primeira coordenada para comparação</param>
        /// <param name="b"> Segunda coordenada para comparação</param>
        /// <returns> bool </returns>
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

        /// <summary>
        /// Sobrecarga de operador retorna o inverso do método 'operator =='
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static bool operator !=(GeographicCoordinate a, GeographicCoordinate b)
        {
            return !(a == b);
        }

        /// <summary>
        /// Equals: sobrecarga do método Equals padrão para testar se dois objetos GeographicCoordinate 
        /// são iguais dentro da tolerância configurada.
        /// </summary>
        /// <param name="obj"> Objeto contra o qual o objeto chamador do método sera comparado. </param>
        /// <returns> bool </returns>
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

    /// <summary>
    /// Brazil: Classe gerencia conversão de sigla para nome do estado brasileiro correspondente
    /// </summary>
    
    public class Brazil
    {
        /// <summary>
        /// Retorna o nome do estado brasileiro correspondente a sigla do estado fornecida
        /// </summary>
        /// <param name="state"> Sigla representando um estado brasileiro </param>
        /// <returns> Nome do estao brasileiro </returns>
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

    /// <summary>
    /// DbGeographyUtil: Classe armazena as informações extraídas do arquivo KML dos municípios num dicionário
    /// cuja chave é uma string que agrega as informações do município. Veja <see cref="formatCityKeyFormatted"/>.
    /// </summary>

    class DbGeographyUtil
    {
        /// <summary>
        /// cityCoordinatesList: dicionario segura informações de posicionamento de cidades. A estrutura 
        /// KeyValuePair<String, DbGeography> corresponde à informações da cidade agregadas numa string e 
        /// à um objeto do tipo que armazena informações do polígono geográfico relativo à cidade, respectivamente.
        /// </summary>
        Dictionary<String, DbGeography> cityCoordinatesList = new Dictionary<String, DbGeography>();

        /// <summary>
        /// Retorna um dicionario contendo os polígonos das cidades brasileiras. Chave do dicionário: nome da cidade
        /// </summary>
        public Dictionary<String, DbGeography> Coordenadas
        {
            get
            {
                return cityCoordinatesList;
            }
        }

        /// <summary>
        /// formatCityKeyFormatted: cria uma string contendo as informações da cidade em questão. 
        /// Esse método foi criado para unificar numa string todas as informações a serem 
        /// preenchidas no documento MongoDB
        /// </summary>
        /// <param name="cityName">Auto</param>
        /// <param name="estado">Auto</param>
        /// <param name="sigla">Auto</param>
        /// <param name="mesoregiao">Auto</param>
        /// <param name="nomemeso">Auto</param>
        /// <param name="microregiao">Auto</param>
        /// <param name="nomemicro">Auto</param>
        /// <returns> string </returns>
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

        /// <summary>
        /// buscaCidadePorLatLongAsync: Cria Task para ser utilizada numa chamada assíncrona. 
        /// Retorna o nome da cidade cuja coordenada recebida como parâmetro intercepte o (ou seja interna ao) polígono.
        /// </summary>
        /// <param name="coord"> coordenada (Longitude, Latitude) da cidade desejada </param>
        /// <returns> Task<string> que realiza a busca no dicionario cidades, coordenadas </string></returns>
        public Task<string> buscaCidadePorLatLongAsync(DbGeography coord)
        {
            try
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
            } catch (AggregateException ae)
            {
                Console.WriteLine("Exception: " + ae.ToString());
                return null;
            }
        }

        /// <summary>
        /// buscaCidadePorLatLong: versão síncrona do método buscaCidadePorLatLongAsync. 
        /// Retorna o nome da cidade cuja coordenada recebida como parâmetro intercepte o (ou seja interna ao) polígono.
        /// </summary>
        /// <param name="coord"> coordenada (Longitude, Latitude) da cidade desejada </param>
        /// <returns> string contendo o nome da cidade </returns>
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

        /// <summary>
        /// ConvertLatLonToDbGeography: 
        /// </summary>
        /// <param name="longitude"></param>
        /// <param name="latitude"></param>
        /// <returns> string contendo um 'Well-known_text' de PONTO geográfico para criação de objeto DbGeography
        /// https://en.wikipedia.org/wiki/Well-known_text </returns>
        public static DbGeography ConvertLatLonToDbGeography(double longitude, double latitude)
        {
            var point = string.Format("POINT({1} {0})", latitude, longitude);
            return DbGeography.FromText(point);
        }

        /// <summary>
        /// ConvertStringArrayToGeographicCoordinates: Cria objeto GeographicCoordinate a partir de uma string 
        /// formato 'longitude, latitude'. 
        /// </summary>
        /// <param name="pointString"></param>
        /// <returns>Objeto GeographicCoordinate relativo ao PONTO </returns>
        public static GeographicCoordinate ConvertStringArrayToGeographicCoordinates(string pointString)
        {
            var points = pointString.Split(',');
            var coordinates = new GeographicCoordinate(double.Parse(points[0]), double.Parse(points[1]));

            return coordinates;
        }

        /// <summary>
        /// ConvertGeoCoordinatesToPolygon: 
        /// </summary>
        /// <param name="coordinates"> Lista de objetos do tipo GeographicCoordinate das coordenadas que delimitam 
        /// o polígono da cidade desejada 
        /// </param>
        /// <returns>string contendo um 'Well-known_text' de POLIGONO geográfico para criação de objeto DbGeography.
        /// https://en.wikipedia.org/wiki/Well-known_text </returns>
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

        /// <summary>
        /// Fence_ImportBrazilianCitiesFromKMLFile: 
        /// </summary>
        /// <param name="path_to_kml_file"></param>
        public void Fence_ImportBrazilianCitiesFromKMLFile(string path_to_kml_file)
        {
            int importCounter = 0;
            StringBuilder errorMessages = new StringBuilder();

            using (StreamReader fileReader = new StreamReader(path_to_kml_file))
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

        /// <summary>
        /// GetCoordinateFromFormattedString: Retorna objeto DbGEography do polígono referente a cidade
        /// das coordenadas passadas como parâmtro.
        /// </summary>
        /// <param name="pontos"> Lista de strings contendo PONTOS 'well-known_text' das coordenadas </param>
        /// <returns> Objeto DbGeography do polígono </returns>
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

        /// <summary>
        /// GetCoordinatesFromKMLPolygonElement: 
        /// </summary>
        /// <param name="polygonElement"></param>
        /// <returns></returns>
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
        /// <summary>
        /// resolveDocumentos: 
        /// </summary>
        /// <param name="carroConectado"> Objeto de referência da coleção MongoDB 'carroConectado'</param>
        /// <param name="objUtil"> Objeto contendo as informações carregadas do arquivo KML</param>
        /// <param name="filter"> Objeto contendo o filtro para consulta de documentos no MongoDB</param>
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
                catch (AggregateException e)
                {
                    e.Handle((x) =>
                    {
                        BsonDocument data = (BsonDocument)document["Data"];
                        BsonDocument pos = (BsonDocument)data["Position"];
                        BsonArray point = (BsonArray)pos["Point"];

                        Console.WriteLine("Erro: {1} {0}", point[0], point[1]);
                        return false;
                    });
                }

            }).Wait();
        }

        static void Main(string[] args)
        {
            var mongoClient = new MongoClient("mongodb://localhost:27017");
            var database = mongoClient.GetDatabase("local");

            IMongoCollection<BsonDocument> carroConectado = database.GetCollection<BsonDocument>("carroconectado20161003_2");
            
            DbGeographyUtil objUtil = new DbGeographyUtil();

            objUtil.Fence_ImportBrazilianCitiesFromKMLFile("C:\\Users\\Pablo\\Downloads\\brasil.kml");

            // FIXME: trata objetos que nao foram atualizados na base de dados
            // Coordenadas nao sao "matched" com as coordenadas do KML fornecido
            var filter = new BsonDocument("Data.Position.Cidade", new BsonDocument("$exists", false));

            //var filter = new BsonDocument();

            resolveDocumentos(carroConectado, objUtil, filter);
        }
    }
}
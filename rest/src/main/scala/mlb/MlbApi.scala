package mlb

import zio._
import zio.jdbc._
import zio.http._
import com.github.tototoshi.csv._
import zio.stream._

import java.sql.Date
import java.io.File
import java.time.LocalDate
import scala.annotation.tailrec
import mlb.Element.*

object MlbApi extends ZIOAppDefault {

  import DataService._
  import ApiService._
  import HomeTeams._
  import AwayTeams._

  val static: App[Any] = Http.collect[Request] {
    case Method.GET -> Root / "text" => Response.text("Hello MLB Fans!")
    case Method.GET -> Root / "json" => Response.json("""{"greetings": "Hello MLB Fans!"}""")
  }.withDefaultErrorResponse

  val endpoints: App[ZConnectionPool] = Http.collectZIO[Request] {
    case Method.GET -> Root / "game" / "latest" / homeTeam / awayTeam =>
      for {
        game: Option[Game] <- latest(HomeTeam(homeTeam), AwayTeam(awayTeam))
        res: Response = latestGameResponse(game)
      } yield res
    case Method.GET -> Root / "game" / "predict" / homeTeam / awayTeam =>
      for {
        game: Option[Prediction] <- getProbaWinTeam(HomeTeam(homeTeam), AwayTeam(awayTeam))
        res: Response = latestPredictionResponse(game)
      } yield res
    case Method.GET -> Root / "games" / "count" =>
      for {
        count: Option[Int] <- count
        res: Response = countResponse(count)
      } yield res
    case Method.GET -> Root / "games" / "history" / homeTeam =>
      import zio.json.EncoderOps
      import Game._
      for {
        listGames: List[Game] <- getAllGamesOfHomeTeam(HomeTeam(homeTeam))
        res: Response = allGameResponse(listGames)
      } yield res
    case _ =>
      ZIO.succeed(Response.text("Not Found").withStatus(Status.NotFound))
  }.withDefaultErrorResponse

  val appLogic: ZIO[ZConnectionPool & Server, Throwable, Unit] = for {
    _ <- createGames *> createPredictions *> initDB("/Users/romainmarques/Documents/GitHub/scala-backend/files/mlb_elo_latest.csv") *> selectEverything
    _ <- Server.serve[ZConnectionPool](static ++ endpoints)
    
  } yield ()

  override def run: ZIO[Any, Throwable, Unit] =
    appLogic.provide(createZIOPoolConfig >>> connectionPool, Server.default)
}

object ApiService {

  import zio.json.EncoderOps
  import Game._
  import Prediction._

  def countResponse(count: Option[Int]): Response = {
    count match
      case Some(c) => Response.text(s"$c game(s) in historical data").withStatus(Status.Ok)
      case None => Response.text("No game in historical data").withStatus(Status.NotFound)
  }

  def latestGameResponse(game: Option[Game]): Response = {
    println(game)
    game match
      case Some(g) => Response.json(g.toJson).withStatus(Status.Ok)
      case None => Response.text("No game found in historical data").withStatus(Status.NotFound)
  }

  def latestPredictionResponse(predi: Option[Prediction]): Response = {
    predi match
      case Some(p) => Response.text(s"${p.homeTeam} vs ${p.awayTeam} winning probability : ${p.homeTeamWinningProb()}").withStatus(Status.Ok)
      case None => Response.text("No game found in historical data").withStatus(Status.NotFound)
  }

  def allGameResponse(games: List[Game]): Response = {
    @tailrec
    def getAllGames(games: List[Game], acc: String): Response = {
      games match
        case x::xs => getAllGames(xs, acc + x.toJson + "\n")
        case Nil => Response.json(acc).withStatus(Status.Ok)
    }
    getAllGames(games, "")
  }
}

object DataService {

  val createZIOPoolConfig: ULayer[ZConnectionPoolConfig] =
    ZLayer.succeed(ZConnectionPoolConfig.default)

  val properties: Map[String, String] = Map(
    "user" -> "postgres",
    "password" -> "postgres"
  )

  val connectionPool: ZLayer[ZConnectionPoolConfig, Throwable, ZConnectionPool] =
    ZConnectionPool.h2mem(
      database = "mlb",
      props = properties
    )

  val createGames: ZIO[ZConnectionPool, Throwable, Unit] = transaction {
    execute(
      sql"CREATE TABLE IF NOT EXISTS games(date DATE NOT NULL, season_year INT NOT NULL, playoff_round INT, home_team VARCHAR(3), away_team VARCHAR(3))"
    )
  }

  val createPredictions: ZIO[ZConnectionPool, Throwable, Unit] = transaction {
    execute(
      sql"CREATE TABLE IF NOT EXISTS predictions(date DATE NOT NULL, season INT NOT NULL, homeTeam VARCHAR(3), awayTeam VARCHAR(3), homeTeamEloProb FLOAT, homeTeamRatingProb FLOAT);"
    )
  }
  val select: ZIO[ZConnectionPool, Throwable, List[Game]] = transaction {
    selectAll(
      sql"SELECT * FROM games LIMIT 10".as[Game]
    ).map(_.toList)
  }

  import GameDates.*
  import PlayoffRounds.*
  import SeasonYears.*
  import HomeTeams.*
  import AwayTeams.*
  import HomeTeamsEloProb.*
  import HomeTeamsRatingProb.*

  def initDB(csvPath: String): ZIO[ZConnectionPool, Throwable, List[Game]] = {
     for {
      _ <- createGames *> createPredictions
      source <- ZIO.succeed(CSVReader.open(new File(csvPath))) // Open CSV
      stream <- ZStream
        .fromIterator[Seq[String]](source.iterator)
        .map[Option[Element]](transformElement)
        .collectSome[Element]
        .grouped(2)
        .foreach(chunk => {
          insertGames(chunk.toList.map(line => line.game)) *> insertPredictions(chunk.toList.map(line => line.prediction))
          }
        )
      _ <- ZIO.succeed(source.close())
      res <- select
    } yield res
  }

  def transformElement(line: Seq[String]): Option[Element] = {
    line match
      case line if line.head == "date" => {
        Console.printLine("header", line)
        None
      }
      case line => {
        Console.printLine("line", line)
        Some(Element(
          Game(
            GameDate(LocalDate.parse(line(0))),
            SeasonYear(line(1).toInt),
            line(3).trim.toIntOption.map(
              PlayoffRound.apply
            ),
            HomeTeam(line(4)),
            AwayTeam(line(5))
          ),
          Prediction(
            GameDate(LocalDate.parse(line(0))),
            SeasonYear(line(1).toInt),
            HomeTeam(line(4)),
            AwayTeam(line(5)),
            HomeTeamEloProb(line(8).toFloat),
            HomeTeamRatingProb(line(20).toFloat)
          )
        ))
      }
  }

  def insertGames(allGames: List[Game]): ZIO[ZConnectionPool, Throwable, UpdateResult] = {
    val rows: List[Game.Row] = allGames.map(_.toRow)
    transaction {
      insert(
        sql"INSERT INTO games(date, season_year, playoff_round, home_team, away_team)".values[Game.Row](rows)
      )
    }
  }

  def insertPredictions(allPredictions: List[Prediction]): ZIO[ZConnectionPool, Throwable, UpdateResult] = {
    val rows: List[Prediction.Row] = allPredictions.map(_.toRow)
    transaction {
      insert(
        sql"INSERT INTO predictions(date, season, homeTeam, awayTeam, homeTeamEloProb, homeTeamRatingProb)".values[Prediction.Row](rows)
      )
    }
  }

  val count: ZIO[ZConnectionPool, Throwable, Option[Int]] = transaction {
    selectOne(
      sql"SELECT COUNT(*) FROM games".as[Int]
    )
  }

  def latest(homeTeam: HomeTeam, awayTeam: AwayTeam): ZIO[ZConnectionPool, Throwable, Option[Game]] = {
    transaction {
      selectOne(
        sql"SELECT date, season_year, playoff_round, home_team, away_team FROM games WHERE home_team = ${HomeTeam.unapply(homeTeam)} AND away_team = ${AwayTeam.unapply(awayTeam)} ORDER BY date DESC LIMIT 1".as[Game]
      )
    }
  }

  def getAllGamesOfHomeTeam(homeTeam: HomeTeam): ZIO[ZConnectionPool, Throwable, List[Game]] = {
    transaction {
      selectAll(
        sql"SELECT date, season_year, playoff_round, home_team, away_team FROM games WHERE home_team = ${HomeTeam.unapply(homeTeam)} ORDER BY date DESC".as[Game]
      ).map(_.toList)
    }
  }
  def getProbaWinTeam(homeTeam: HomeTeam, awayTeam: AwayTeam): ZIO[ZConnectionPool, Throwable, Option[Prediction]] = {
    transaction {
      selectOne(
        sql"SELECT date, season, homeTeam, awayTeam, homeTeamEloProb, homeTeamRatingProb FROM predictions WHERE homeTeam = ${HomeTeam.unapply(homeTeam)} and awayTeam = ${AwayTeam.unapply(awayTeam)} ORDER BY date DESC LIMIT 1".as[Prediction]
      )
    }
  }

  def selectEverything : ZIO[ZConnectionPool, Throwable, List[Game]] = {
    transaction {
      selectAll(
        sql"SELECT date, season_year, playoff_round, home_team, away_team FROM games".as[Game]
      ).map(_.toList)
    }
  }
}

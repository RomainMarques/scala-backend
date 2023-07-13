package mlb

import zio.json._
import zio.jdbc._

import java.time.LocalDate

object HomeTeamsEloProb {
  opaque type HomeTeamEloProb = Float

  object HomeTeamEloProb {
    def apply(value: Float): HomeTeamEloProb = value

    def unapply(homeTeamEloProb: HomeTeamEloProb): Float = homeTeamEloProb
  }

  given CanEqual[HomeTeamEloProb, HomeTeamEloProb] = CanEqual.derived
  implicit val homeTeamEloProbEncoder: JsonEncoder[HomeTeamEloProb] = JsonEncoder.float
  implicit val homeTeamEloProbDecoder: JsonDecoder[HomeTeamEloProb] = JsonDecoder.float
}

object HomeTeamsRatingProb {

  opaque type HomeTeamRatingProb = Float

  object HomeTeamRatingProb {

    def apply(value: Float): HomeTeamRatingProb = value

    def unapply(homeTeamRatingProb: HomeTeamRatingProb): Float = homeTeamRatingProb
  }

  given CanEqual[HomeTeamRatingProb, HomeTeamRatingProb] = CanEqual.derived
  implicit val homeTeamRatingProbEncoder: JsonEncoder[HomeTeamRatingProb] = JsonEncoder.float
  implicit val homeTeamRatingProbDecoder: JsonDecoder[HomeTeamRatingProb] = JsonDecoder.float
}

import HomeTeamsEloProb.*
import HomeTeamsRatingProb.*
import GameDates.*
import SeasonYears.*
import HomeTeams.*
import AwayTeams.*

final case class Prediction(
  date: GameDate,
  season: SeasonYear,
  homeTeam: HomeTeam,
  awayTeam: AwayTeam,
  homeTeamEloProb: HomeTeamEloProb,
  homeTeamRatingProb: HomeTeamRatingProb
)

object Prediction {

  given CanEqual[Prediction, Prediction] = CanEqual.derived
  implicit val gameEncoder: JsonEncoder[Prediction] = DeriveJsonEncoder.gen[Prediction]
  implicit val gameDecoder: JsonDecoder[Prediction] = DeriveJsonDecoder.gen[Prediction]

  def unapply(prediction: Prediction): (GameDate, SeasonYear, HomeTeam, AwayTeam, HomeTeamEloProb, HomeTeamRatingProb) =
    (prediction.date, prediction.season, prediction.homeTeam, prediction.awayTeam, prediction.homeTeamEloProb, prediction.homeTeamRatingProb)

  extension (p: Prediction)
    def homeTeamProb(): Float = mean(p)
  def mean(prediction: Prediction): Float =
    (HomeTeamEloProb.unapply(prediction.homeTeamEloProb) + HomeTeamRatingProb.unapply(prediction.homeTeamRatingProb)) / 2

  // a custom decoder from a tuple
  type Row = (String, Int, String, String, Float, Float)

  extension (p: Prediction)
    def toRow: Row =
      val (d, y, h, a, ep, rp) = Prediction.unapply(p)
      (
        GameDate.unapply(d).toString,
        SeasonYear.unapply(y),
        HomeTeam.unapply(h),
        AwayTeam.unapply(a),
        HomeTeamEloProb.unapply(ep),
        HomeTeamRatingProb.unapply(rp)
      )

  implicit val jdbcDecoder: JdbcDecoder[Prediction] = JdbcDecoder[Row]().map[Prediction] { t =>
      val (date, season, homeTeam, awayTeam, homeTeamEloProb, homeTeamRatingProb) = t
      Prediction(
        GameDate(LocalDate.parse(date)),
        SeasonYear(season),
        HomeTeam(homeTeam),
        AwayTeam(awayTeam),
        HomeTeamEloProb(homeTeamEloProb),
        HomeTeamRatingProb(homeTeamRatingProb)
      )
    }
}

val predictions : List[Prediction] = List (
  Prediction(
    GameDate(LocalDate.parse("2021-04-01")),
    SeasonYear(2021),
    HomeTeam("TOR"),
    AwayTeam("NYY"),
    HomeTeamEloProb(0.5),
    HomeTeamRatingProb(0.5)
  ),
  Prediction(
    GameDate(LocalDate.parse("2021-04-02")),
    SeasonYear(2021),
    HomeTeam("TOR"),
    AwayTeam("NYY"),
    HomeTeamEloProb(0.4),
    HomeTeamRatingProb(0.6)
  )
)
package base


class BaseClass {
}
case class UserBaseInfo(userid:Long,gender:Int,province:Int) extends java.io.Serializable
case class BeUserBaseInfo(userid:Long,gender:Int,province:Int) extends java.io.Serializable
case class Relation(userid:Long,beuserid:Long) extends java.io.Serializable
case class RelationAndScore(userid:Long,beuserid:Long,score:Double) extends java.io.Serializable
// item-based cf or user-based cf using this
case class RelationRatingsWithSize(userid:Long,beuserid:Long,score:Double,size:Int) extends java.io.Serializable
case class TwoSimilarUserOrIrem(userid1:Long,userid2:Long) extends java.io.Serializable
case class TempCalcStating(dotProduct:Double,rating1:Double,rating2:Double,rating1Sq:Double,rating2Sq:Double,rating1Num:Double,rating2Num:Double) extends java.io.Serializable
case class ReduceTempCalcStating(commonSize:Double,dotProductSum:Double,rating1Sum:Double,rating2Sum:Double,rating1SquareSum:Double,rating2SquareSum:Double,rating1Num:Double,rating2Num:Double) extends java.io.Serializable
case class PredictStatistics(userid:Long,beUserid:Long,simValue:Double,dotProduct:Double) extends java.io.Serializable
// similar filter case class
case class SimilarUserInfo(userid:Long,age:Int,province:Int,city:Int,height:Int,education:Int,income:Int) extends java.io.Serializable
// recommend filter case class
case class RecommendUserInfo(userid:Long,age:Int,province:Int,city:Int,height:Int,education:Int,income:Int,housing:Int) extends java.io.Serializable
case class RecommendUserMatchInfo(userid:Long,age:Int,province:Int,city:Int,height:Int,matchMinAge:Int,matchMaxAge:Int,
                                  matchProvince:Int,matchCity:Int,matchEducation:String,matchIncome:String,matchMinHeight:Int,matchMaxHeight:Int,
                                   matchHousing:String) extends java.io.Serializable
//[beview,click,beclick,IMSender,IMReceiver,msgSendeer,msgReceiver]
case class StatisticAction(viewNum:Int,beViewNum:Int,clickNum:Int,beClickedNum:Int,
                           imSenderNum:Int,imReceiverNum:Int,msgSendeerNum:Int,msgReceiverNum:Int) extends java.io.Serializable

case class ActionNum(active1:Int=0,passive1:Int=0,active2:Int=0,passive2:Int=0,active3:Int=0,passive3:Int=0) extends java.io.Serializable

case class ActionStatistic(click:ActionNum=ActionNum(),focus:ActionNum=ActionNum(),msg:ActionNum=ActionNum(),im:ActionNum=ActionNum()) extends java.io.Serializable

//case class ActionStatistic1(click1Num:Int=0,beClicked1Num:Int=0,click2Num:Int=0,beClicked2Num:Int=0,click3Num:Int=0,beClicked3Num:Int=0,
//                     focus1Num:Int=0,beFocused1Num:Int=0,focus2Num:Int=0,beFocused2Num:Int=0,focus3Num:Int=0,beFocused3Num:Int=0,
//                     msgSend1Num:Int=0,msgRecv1Num:Int=0,msgSend2Num:Int=0,msgRecv2Num:Int=0,msgSend3Num:Int=0,msgRecv3Num:Int=0,
//                     imSend1Num:Int=0,imRecv1Num:Int=0,imSend2Num:Int=0,imRecv2Num:Int=0,imSend3Num:Int=0,imRecv3Num:Int=0) extends java.io.Serializable
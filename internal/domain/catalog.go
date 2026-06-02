package domain

// ConditionGrade 表示拍品成色值对象。
type ConditionGrade string

const (
	ConditionNew     ConditionGrade = "NEW"
	ConditionLikeNew ConditionGrade = "LIKE_NEW"
	ConditionGood    ConditionGrade = "GOOD"
	ConditionFair    ConditionGrade = "FAIR"
)

func (g ConditionGrade) Valid() bool {
	switch g {
	case ConditionNew, ConditionLikeNew, ConditionGood, ConditionFair:
		return true
	default:
		return false
	}
}

package filter

type FluxFilter struct {
	Not *FluxFilter
	Or  []*FluxFilter
	And []*FluxFilter

	Measurement       *string
	MeasurementNEQ    *string
	MeasurementMatch  *string
	MeasurementNMatch *string

	Field       *string
	FieldNEQ    *string
	FieldMatch  *string
	FieldNMatch *string

	TagKey    *string
	Tag       *string
	TagNEQ    *string
	TagMatch  *string
	TagNMatch *string
}

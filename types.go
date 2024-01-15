package main

type CDR struct {
	Timestamp         string `json:"timestamp,omitempty"`
	Type              string `json:"type,omitempty"`
	SessionID         string `json:"session_id,omitempty"`
	LegID             string `json:"leg_id,omitempty"`
	StartTime         string `json:"start_time,omitempty"`
	ConnectedTime     string `json:"connected_time,omitempty"`
	EndTime           string `json:"end_time,omitempty"`
	FreedTime         string `json:"freed_time,omitempty"`
	Duration          string `json:"duration,omitempty"`
	TerminationCause  string `json:"termination_cause,omitempty"`
	TerminationSource string `json:"termination_source,omitempty"`
	Calling           string `json:"calling,omitempty"`
	Called            string `json:"called,omitempty"`
	NAP               string `json:"nap,omitempty"`
	Direction         string `json:"direction,omitempty"`
	Media             string `json:"media,omitempty"`
	RtpRx             string `json:"rtp_rx,omitempty"`
	RtpTx             string `json:"rtp_tx,omitempty"`
	T38Rx             string `json:"t_38_rx,omitempty"`
	T38Tx             string `json:"t_38_tx,omitempty"`
	ErrorFromNetwork  string `json:"error_from_network,omitempty"`
	ErrorToNetwork    string `json:"error_to_network,omitempty"`
	MOS               string `json:"mos,omitempty"`
	NetworkQuality    string `json:"network_quality,omitempty"`
}

package firebasenotify

import (
	firebase "firebase.google.com/go"
	"firebase.google.com/go/messaging"
	"context"
	"github.com/duanhf2012/origin/service"
	"github.com/duanhf2012/origin/log"
	"google.golang.org/api/option"
)

type NotifyMsg struct {
	RegistrationTokens []string
	Title string
	Body string
	ImageURL string
	Data map[string]string
}

type FireBaseNotifyService struct {
	service.Service
	app *firebase.App
}

func NewFireBaseNotifyService(serviceAccountKeyFile string,coroutineNum int32) *FireBaseNotifyService{
	notifyService := &FireBaseNotifyService{}
	notifyService.SetGoRouterNum(coroutineNum)
	opt := option.WithCredentialsFile(serviceAccountKeyFile)

 	//,option.WithHTTPClient(NewHttpClient("http://192.168.0.5:1081"))
	app, err := firebase.NewApp(context.Background(), nil, opt)
	if err != nil {
		log.Error("Error initializing firebase app: %v\n", err)
		return nil
	}
	notifyService.app = app

	return notifyService
}

func (slf *FireBaseNotifyService) RPC_Send(notifyMsg *NotifyMsg,batchResponse *messaging.BatchResponse) error {
	client, err := slf.app.Messaging(context.Background())
	if err != nil {
		log.Error("Error getting Messaging client: %v\n", err)
		return err
	}

	msg :=  &messaging.MulticastMessage{
		Tokens:       notifyMsg.RegistrationTokens,
		Data:         nil,
		Notification: &messaging.Notification{Title:notifyMsg.Title,Body:notifyMsg.Body,ImageURL:notifyMsg.ImageURL},
		Android:      nil,
		Webpush:      nil,
		APNS:         nil,
	}
	br, err := client.SendMulticast(context.Background(),msg)
	if br != nil {
		*batchResponse = *br
	}
	if err != nil {
		log.Error("Error SendMulticast :%v\n",err)
		return err
	}

	return nil
}
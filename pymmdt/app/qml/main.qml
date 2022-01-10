import QtQuick 2.12
import QtQuick.Window 2.12
import QtQuick.Controls 2.12


import "timelines"

Window {
    id: root
    width: 1000
    height: 580
    visible: true
    title: qsTr("PyMMDT Dashboard")

    Rectangle {
        id: appContainer
        color: "#282a36"
        anchors.right: parent.right
        anchors.rightMargin: 0
        anchors.left: parent.left
        anchors.leftMargin: 0
        anchors.bottom: parent.bottom
        anchors.bottomMargin: 0
        anchors.top: parent.top
        anchors.topMargin: 0

        Rectangle {
            id: topBar
            height: 52
            color: "#282a36"
            anchors.right: parent.right
            anchors.rightMargin: 0
            anchors.left: parent.left
            anchors.leftMargin: 0
            anchors.top: parent.top
            anchors.topMargin: 0

            Button {
                id: playPauseButton
                x: 450
                width: 50
                icon.source: Manager.is_play ? "resources/icons8-play-30.png" : "resources/icons8-pause-30.png"
                icon.color: "#f8f8f2"
                anchors.bottom: parent.bottom
                anchors.bottomMargin: 6
                anchors.top: parent.top
                anchors.topMargin: 6
                display: AbstractButton.IconOnly
                background: Rectangle {
                    color: "transparent"
                }
                onClicked: Manager.play_pause()
            }

            Button {
                id: appButton
                width: 100
                display: AbstractButton.TextOnly
                transformOrigin: Item.Center
                font.pointSize: 13
                anchors.bottom: parent.bottom
                anchors.bottomMargin: 0
                anchors.left: parent.left
                anchors.leftMargin: 0
                anchors.top: parent.top
                anchors.topMargin: 0
                background: Rectangle {
                    color: "transparent"
                }
                Text {
                    anchors.fill: parent
                    horizontalAlignment: Text.AlignHCenter
                    verticalAlignment: Text.AlignVCenter
                    color: "#f8f8f2"
                    text: qsTr("PyMMDT")
                }
            }

            Button {
                id: settingsButton
                x: 910
                width: 90
                icon.source: "resources/icons8-settings-50.png"
                icon.color: "#f8f8f2"
                anchors.right: parent.right
                anchors.rightMargin: 0
                anchors.bottom: parent.bottom
                anchors.bottomMargin: 0
                anchors.top: parent.top
                anchors.topMargin: 0
                background: Rectangle {
                    color: "transparent"
                }

            }

            Button {
                id: userButton
                x: 148
                width: 100
                anchors.bottom: parent.bottom
                anchors.bottomMargin: 0
                anchors.top: parent.top
                anchors.topMargin: 0
                background: Rectangle {
                    color: "transparent"
                }
                Text {
                    anchors.fill: parent
                    horizontalAlignment: Text.AlignHCenter
                    verticalAlignment: Text.AlignVCenter
                    color: "#f8f8f2"
                    text: qsTr("User-Sort")
                }
            }

            Button {
                id: entryButton
                x: 299
                width: 100
                anchors.top: parent.top
                anchors.topMargin: 0
                anchors.bottom: parent.bottom
                anchors.bottomMargin: 0
                background: Rectangle {
                    color: "transparent"
                }
                Text {
                    anchors.fill: parent
                    horizontalAlignment: Text.AlignHCenter
                    verticalAlignment: Text.AlignVCenter
                    color: "#f8f8f2"
                    text: qsTr("Entry-Sort")
                }
            }
        }

        // Timetrack (container for Timelines)
        Rectangle {
            id: timeTrack
            height: timetrackView.contentHeight
            color: "#44475a"
            anchors.bottom: parent.bottom
            anchors.bottomMargin: 0
            anchors.right: parent.right
            anchors.rightMargin: 0
            anchors.left: parent.left
            anchors.leftMargin: 0

            ListView {
                id: timetrackView
                anchors.fill: timeTrack
                spacing: 0
                orientation: ListView.Vertical
                model: Manager.timetrack_model
                delegate: BaseTimelineDelegate{}
            }

            Rectangle {
                y: 0
                x: SlidingBar.state * (timeTrack.width-30-6) + 30
                width: 6
                height: parent.height
                color: "orange"
            }
        }

        Rectangle {
            id: content
            color: "#44475a"
            anchors.top: topBar.bottom
            anchors.topMargin: 0
            anchors.bottom: timeTrack.top
            anchors.bottomMargin: 0
            anchors.right: parent.right
            anchors.rightMargin: 0
            anchors.left: parent.left
            anchors.leftMargin: 0

            Loader {
                id: pageLoader
                anchors.fill: parent
                source: Qt.resolvedUrl("pages/homePage.qml")
            }
        }
    }
}




